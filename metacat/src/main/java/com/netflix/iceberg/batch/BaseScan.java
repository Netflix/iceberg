/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.netflix.iceberg.batch;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.JsonNode;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.source.PartitionKey;
import org.apache.iceberg.spark.source.StructInternalRow;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.execution.datasources.FileStatusCache;
import org.apache.spark.sql.execution.datasources.PartitionDirectory;
import org.apache.spark.sql.execution.datasources.PartitionPath;
import org.apache.spark.sql.execution.datasources.PartitionSpec;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.PrunedInMemoryFileIndex;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

abstract class BaseScan implements Scan, Batch, SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(BaseScan.class);
  private static final Statistics EMPTY_STATS = new Statistics() {
    @Override
    public OptionalLong sizeInBytes() {
      return OptionalLong.empty();
    }

    @Override
    public OptionalLong numRows() {
      return OptionalLong.empty();
    }
  };

  static final String[] NO_LOCALITY = new String[0];

  private final SparkSession spark;
  private final MetacatSparkTable table;
  private final StructType expectedSchema;
  private final List<Expression> partFilters;
  private final List<PartitionDto> partitions;
  private final Long numRows;
  private final Long totalSize;
  private final PrunedInMemoryFileIndex fileIndex;
  private final Map<String, String> options;
  private final long splitSize;
  private final long openCost;
  private final int lookback;

  BaseScan(SparkSession spark, MetacatSparkTable table, StructType expectedSchema, List<Expression> partFilters,
           List<PartitionDto> partitions, Map<String, String> readOptions) {
    this.spark = spark;
    this.table = table;
    this.expectedSchema = SparkTables.expectedProjection(expectedSchema, table.partitionSchema());
    this.partFilters = partFilters;
    this.options = BatchUtil.buildTableOptions(table.options(), readOptions);
    this.partitions = partitions;
    this.numRows = accumulateRowCount(partitions);
    this.totalSize = accumulateTotalSize(partitions);
    this.fileIndex = buildFileIndex(spark, table, partitions);

    // configuration precedence: first use read options, then table properties, then Spark environment
    this.splitSize = PropertyUtil.propertyAsLong(readOptions, "split-size",
        PropertyUtil.propertyAsLong(table.properties(),
            TableProperties.SPLIT_SIZE, spark.sessionState().conf().filesMaxPartitionBytes()));
    this.openCost = PropertyUtil.propertyAsLong(readOptions, "file-open-cost",
        PropertyUtil.propertyAsLong(table.properties(),
            TableProperties.SPLIT_OPEN_FILE_COST, spark.sessionState().conf().filesOpenCostInBytes()));
    this.lookback = PropertyUtil.propertyAsInt(readOptions, "lookback",
        PropertyUtil.propertyAsInt(table.properties(),
            TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT));
  }

  SparkSession spark() {
    return spark;
  }

  MetacatSparkTable table() {
    return table;
  }

  List<PartitionDto> partitions() {
    return partitions;
  }

  Map<String, String> options() {
    return options;
  }

  @Override
  public StructType readSchema() {
    return expectedSchema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    BinPacking.ListPacker<PartitionedFile> packer = new BinPacking.ListPacker<>(splitSize, lookback, true);

    // pass no filters because the index was built by passing filters to Metacat
    List<PartitionDirectory> partitions = ScalaUtil.asJava(fileIndex.listFiles(ScalaUtil.nil(), ScalaUtil.nil()));

    List<PartitionedFile> files = partitions.stream()
        .flatMap(part -> JavaConverters.seqAsJavaListConverter(part.files()).asJava().stream().map(
            stat -> PartitionedFile.apply(part.values(), stat.getPath().toString(), 0, stat.getLen(), NO_LOCALITY)))
        .flatMap(file -> split(file, splitSize))
        .collect(Collectors.toList());

    return packer.pack(files, file -> file.length() + openCost).stream()
        .map(ReadTask::new)
        .toArray(ReadTask[]::new);
  }

  protected boolean isSplittable() {
    return false;
  }

  private Stream<PartitionedFile> split(PartitionedFile file, long splitSize) {
    if (!isSplittable()) {
      return Stream.of(file);
    }

    List<PartitionedFile> splits = Lists.newArrayList();

    long offset = 0;
    long remainingLen = file.length();
    while (remainingLen > 0) {
      long len = Math.min(splitSize, remainingLen);
      splits.add(PartitionedFile.apply(file.partitionValues(), file.filePath(), offset, len, file.locations()));
      offset += len;
      remainingLen -= len;
    }

    return splits.stream();
  }

  @Override
  public Statistics estimateStatistics() {
    Expression partFilter = partFilters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);

    if (numRows != null) {
      int rowSizeEstimate = Arrays.stream(expectedSchema.fields())
          .map(field -> field.dataType().defaultSize())
          .reduce(0, Integer::sum);
      LOG.info("Estimated {} rows, {} bytes for table {} with filter {}",
          numRows, rowSizeEstimate * numRows, table.name(), partFilter);

      return new Statistics() {
        @Override
        public OptionalLong sizeInBytes() {
          return OptionalLong.of(numRows * rowSizeEstimate);
        }

        @Override
        public OptionalLong numRows() {
          return OptionalLong.of(numRows);
        }
      };

    } else if (totalSize != null) {
      LOG.info("Estimated {} bytes (missing row counts) for table {} with filter {}",
          totalSize, table.name(), partFilter);

      return new Statistics() {
        @Override
        public OptionalLong sizeInBytes() {
          return OptionalLong.of(totalSize);
        }

        @Override
        public OptionalLong numRows() {
          return OptionalLong.empty();
        }
      };

    } else {
      LOG.info("Could not estimate rows for table {} with filter {}", table.name(), partFilter);
      return EMPTY_STATS;
    }
  }

  @Override
  public String description() {
    String filters = partFilters.stream().map(SparkUtil::describe).collect(Collectors.joining(", "));
    return String.format("%s [filters=%s]", table.name(), filters);
  }

  static class ReadTask implements InputPartition {
    private final PartitionedFile[] files;

    ReadTask(List<PartitionedFile> files) {
      this.files = files.toArray(new PartitionedFile[0]);
    }

    PartitionedFile[] files() {
      return files;
    }

    @Override
    public String[] preferredLocations() {
      return new String[0];
    }
  }

  abstract static class TaskReader<T> implements PartitionReader<T> {
    private final Iterator<PartitionedFile> files;

    private Closeable currentCloseable = BatchUtil.noopCloseable();
    private Iterator<InternalRow> currentIterator = Collections.emptyIterator();
    private T current = null;

    TaskReader(ReadTask task) {
      this.files = Iterators.forArray(task.files());
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean next() throws IOException {
      while (true) {
        if (currentIterator.hasNext()) {
          // this unsafe cast is needed because Spark's iterator will return either an InternalRow or a ColumnarBatch,
          // but does not use type parameters to distinguish between the two. Instead, it uses type erasure and codegen
          // is responsible for knowing what will come out of the iterator.
          this.current = (T) currentIterator.next();
          return true;

        } else if (files.hasNext()) {
          this.currentCloseable.close();
          this.currentIterator = openFile(files.next());

        } else {
          return false;
        }
      }
    }

    @Override
    public T get() {
      return current;
    }

    @Override
    public void close() throws IOException {
      InputFileBlockHolder.unset();

      // close the current iterator
      this.currentCloseable.close();

      // exhaust the task iterator
      while (files.hasNext()) {
        files.next();
      }
    }

    void setCloseable(Object toClose) {
      if (toClose instanceof Closeable) {
        this.currentCloseable = (Closeable) toClose;
      } else {
        this.currentCloseable = BatchUtil.noopCloseable();
      }
    }

    private Iterator<InternalRow> openFile(PartitionedFile file) {
      InputFileBlockHolder.set(file.filePath(), file.start(), file.length());
      return open(file);
    }

    abstract Iterator<InternalRow> open(PartitionedFile file);
  }

  private static Long accumulateRowCount(List<PartitionDto> partitions) {
    long totalRowCount = 0L;

    for (PartitionDto part : partitions) {
      if (part.getDataMetadata() != null) {
        Long partitionRowCount = extractRowCount(part.getDataMetadata().get("metrics"));
        if (partitionRowCount != null) {
          totalRowCount += partitionRowCount;
        } else {
          return null;
        }
      } else {
        return null;
      }
    }

    return totalRowCount;
  }

  private static Long accumulateTotalSize(List<PartitionDto> partitions) {
    long totalSize = 0L;

    for (PartitionDto part : partitions) {
      if (part.getDataMetadata() != null) {
        Long partitionSize = extractPartitionSize(part.getDataMetadata().get("metrics"));
        if (partitionSize != null) {
          totalSize += partitionSize;
        } else {
          return null;
        }
      } else {
        return null;
      }
    }

    return totalSize;
  }

  private static PrunedInMemoryFileIndex buildFileIndex(SparkSession spark, MetacatSparkTable table,
                                                        List<PartitionDto> partitions) {
    List<PartitionPath> paths = partitions.stream()
        .map(part -> PartitionPath.apply(partitionTuple(table.spec(), part), part.getDataUri()))
        .collect(Collectors.toList());

    PartitionSpec sparkPartitioning = PartitionSpec.apply(
        (StructType) SparkSchemaUtil.convert(table.spec().partitionType()),
        JavaConverters.asScalaBufferConverter(paths).asScala());

    return new PrunedInMemoryFileIndex(
        spark, new Path(table.location()), FileStatusCache.getOrCreate(spark), sparkPartitioning, Option.empty());
  }

  static InternalRow partitionTuple(org.apache.iceberg.PartitionSpec spec, PartitionDto part) {
    int numFields = spec.fields().size();
    if (numFields < 1) {
      return new GenericInternalRow(0);
    }

    String partitionPath = part.getName().getPartitionName();
    String[] partitions = partitionPath.split("/", -1);
    Preconditions.checkArgument(partitions.length <= numFields,
        "Invalid partition data, too many fields (expecting %s): %s", numFields, partitionPath);
    Preconditions.checkArgument(partitions.length >= numFields,
        "Invalid partition data, not enough fields (expecting %s): %s", numFields, partitionPath);

    PartitionKey key = new PartitionKey(spec);

    for (int i = 0; i < partitions.length; i += 1) {
      PartitionField field = spec.fields().get(i);
      String[] parts = partitions[i].split("=", 2);
      Preconditions.checkArgument(
          parts.length == 2 && parts[0] != null && field.name().equals(parts[0]),
          "Invalid partition, expected %s: %s", field.name(), partitions[i]);

      key.set(i, Conversions.fromPartitionString(
          spec.partitionType().fields().get(i).type(),
          ExternalCatalogUtils.unescapePathName(parts[1])));
    }

    return new StructInternalRow(spec.partitionType()).setStruct(key);
  }

  private static Long extractPartitionSize(JsonNode metrics) {
    if (metrics != null) {
      JsonNode compressedSizeMetric = metrics.get("com.netflix.dse.mds.metric.CompressedBytes");
      if (compressedSizeMetric != null) {
        JsonNode value = compressedSizeMetric.get("value");
        if (value != null && value.canConvertToLong()) {
          return value.longValue();
        }
      }
    }
    return null;
  }

  private static Long extractRowCount(JsonNode metrics) {
    if (metrics != null) {
      JsonNode rowCountMetric = metrics.get("com.netflix.dse.mds.metric.RowCount");
      if (rowCountMetric != null) {
        JsonNode value = rowCountMetric.get("value");
        if (value != null && value.canConvertToLong()) {
          return value.longValue();
        }
      }
    }
    return null;
  }
}
