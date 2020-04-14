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

import com.google.common.collect.Sets;
import com.netflix.metacat.common.dto.PartitionDto;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Function1;
import scala.collection.JavaConverters;

class FileFormatScan extends BaseScan {
  private static final Set<String> SPLITTABLE_PROVIDERS = Sets.newHashSet("parquet", "avro", "orc");
  private final Filter[] pushedFilters;

  FileFormatScan(SparkSession spark, MetacatSparkTable table, StructType expectedSchema, Filter[] pushedFilters,
                 List<Expression> partFilters, List<PartitionDto> partitions, Map<String, String> readOptions) {
    super(spark, table, expectedSchema, partFilters, partitions, readOptions);
    this.pushedFilters = pushedFilters;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new FileFormatReaderFactory(spark(), table(), readSchema(), Arrays.asList(pushedFilters), options());
  }

  @Override
  protected boolean isSplittable() {
    return SPLITTABLE_PROVIDERS.contains(table().provider());
  }

  private static class FileFormatReaderFactory implements PartitionReaderFactory {
    private final Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> buildReaderFunc;
    private final boolean isColumnar;

    private FileFormatReaderFactory(SparkSession spark, MetacatSparkTable table,
                                    StructType expectedSchema, List<Filter> rowFilters,
                                    Map<String, String> options) {
      org.apache.spark.sql.execution.datasources.FileFormat format = table.format();
      this.buildReaderFunc = format.buildReaderWithPartitionValues(
          spark, table.schema(),
          table.partitionSchema(),
          SparkTables.dataProjection(expectedSchema, table.partitionSchema()), // must exclude partition columns
          JavaConverters.asScalaBufferConverter(rowFilters).asScala(), ScalaUtil.asScala(options),
          spark.sessionState().newHadoopConf());
      this.isColumnar = format.supportBatch(spark, expectedSchema);
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      return new FileFormatReader<>(buildReaderFunc, (ReadTask) partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      return new FileFormatReader<>(buildReaderFunc, (ReadTask) partition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return isColumnar;
    }
  }

  private static class FileFormatReader<T> extends TaskReader<T> {
    private final Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> openFunc;

    FileFormatReader(Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> openFunc, ReadTask task) {
      super(task);
      this.openFunc = openFunc;
    }

    @Override
    Iterator<InternalRow> open(PartitionedFile file) {
      scala.collection.Iterator<InternalRow> scalaIter = openFunc.apply(file);
      setCloseable(scalaIter);
      return JavaConverters.asJavaIteratorConverter(scalaIter).asJava();
    }
  }
}
