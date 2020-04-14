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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.netflix.metacat.common.dto.PartitionDto;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.util.Pair;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.hive.HadoopTableReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Seq;

public class InputFormatScan extends BaseScan {
  private static final Logger LOG = LoggerFactory.getLogger(InputFormatScan.class);
  private static final Joiner SLASH = Joiner.on('/');

  private final Configuration conf;
  private final String tableDeserializerClass;
  private final Properties tableProperties;
  private final StructType dataSchema;

  InputFormatScan(SparkSession spark, MetacatSparkTable table, StructType expectedSchema, List<Expression> partFilters,
                  List<PartitionDto> partitions, Map<String, String> readOptions) {
    super(spark, table, expectedSchema, partFilters, partitions, readOptions);
    this.conf = MapReduceUtil.newJob(buildConf(readOptions), table.location()).getConfiguration();
    this.tableDeserializerClass = table.info().getSerde().getSerializationLib();
    this.tableProperties = HiveUtil.buildTableProperties(table.info());
    this.dataSchema = SparkTables.dataProjection(expectedSchema, table.partitionSchema());
  }

  private Configuration buildConf(Map<String, String> readOptions) {
    Configuration conf = SparkTables.buildConf(spark(), readOptions);

    conf.set("io.file.buffer.size", spark().conf().get("spark.buffer.size", "65536"));

    return conf;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    JavaSparkContext sc = new JavaSparkContext(spark().sparkContext());
    Broadcast<SerializableConfiguration> confBroadcast = sc.broadcast(new SerializableConfiguration(conf));

    return new InputFormatReaderFactory(confBroadcast, tableProperties, tableDeserializerClass, dataSchema);
  }

  @Override
  public InputPartition[] planInputPartitions() {
    StructType partitionStruct = table().partitionSchema();
    ImmutableMap.Builder<String, PartitionInfo> builder = ImmutableMap.builder();
    partitions().forEach(part -> builder.put(
        key(partitionStruct, partitionTuple(table().spec(), part)),
        new PartitionInfo(tableProperties, part)));
    Map<String, PartitionInfo> partitionInfo = builder.build();

    return Stream.of(super.planInputPartitions())
        .map(ReadTask.class::cast)
        .map(task -> new InputFormatReadTask(task, partitionStruct, partitionInfo))
        .toArray(InputPartition[]::new);
  }

  private static class PartitionInfo implements Serializable {
    private final String inputFormatClass;
    private final String partitionDeserializerClass;
    private final Properties partitionProperties;

    transient private InputFormat<Writable, Writable> lazyInputFormat = null;

    private PartitionInfo(Properties tableProperties, PartitionDto partition) {
      this.inputFormatClass = partition.getSerde().getInputFormat();
      this.partitionDeserializerClass = partition.getSerde().getSerializationLib();
      this.partitionProperties = HiveUtil.buildPartitionProperties(tableProperties, partition);
    }

    InputFormat<Writable, Writable> inputFormat(Configuration conf) {
      if (lazyInputFormat == null) {
        this.lazyInputFormat = HiveUtil.newInstance(inputFormatClass, InputFormat.class, conf);
      }
      return lazyInputFormat;
    }

    Deserializer newDeserializer(Properties tableProperties, Configuration conf) {
      return HiveUtil.newDeserializer(partitionDeserializerClass, tableProperties, partitionProperties, conf);
    }
  }

  private static class InputFormatReadTask implements InputPartition {
    private ReadTask task;
    private StructType partitionType;
    private String[] partKeys;
    private PartitionInfo[] partitions;

    transient private Map<String, PartitionInfo> lazyInfoMap = null;

    InputFormatReadTask(ReadTask task, StructType partitionType, Map<String, PartitionInfo> infos) {
      this.task = task;
      this.partitionType = partitionType;

      // filter the incoming map to only the partitions referenced by this task
      List<Pair<String, PartitionInfo>> entries = Stream.of(task.files())
          .map(file -> key(partitionType, file.partitionValues()))
          .collect(Collectors.toSet()).stream()
          .map(key -> Pair.of(key, infos.get(key)))
          .collect(Collectors.toList());

      this.partKeys = entries.stream().map(Pair::first).toArray(String[]::new);
      this.partitions = entries.stream().map(Pair::second).toArray(PartitionInfo[]::new);
    }

    ReadTask readTask() {
      return task;
    }

    PartitionInfo info(InternalRow partitionTuple) {
      return lazyInfoMap().get(key(partitionType, partitionTuple));
    }

    private Map<String, PartitionInfo> lazyInfoMap() {
      if (lazyInfoMap == null) {
        ImmutableMap.Builder<String, PartitionInfo> builder = ImmutableMap.builder();
        for (int i = 0; i < partKeys.length; i += 1) {
          builder.put(partKeys[i], partitions[i]);
        }
        this.lazyInfoMap = builder.build();
      }
      return lazyInfoMap;
    }
  }

  private static class InputFormatReaderFactory implements PartitionReaderFactory {
    private final Broadcast<SerializableConfiguration> confBroadcast;
    private final Properties tableProperties;
    private final String tableDeserializerClass;
    private final StructType dataSchema;
    private final long batchId;

    private InputFormatReaderFactory(Broadcast<SerializableConfiguration> confBroadcast, Properties tableProperties,
                                     String tableDeserializerClass, StructType dataSchema) {
      this.confBroadcast = confBroadcast;
      this.tableProperties = tableProperties;
      this.tableDeserializerClass = tableDeserializerClass;
      this.dataSchema = dataSchema;
      this.batchId = System.currentTimeMillis();
    }

    Configuration conf() {
      return confBroadcast.getValue().value();
    }

    StructType dataSchema() {
      return dataSchema;
    }

    long batchId() {
      return batchId;
    }

    int stageId() {
      return TaskContext.get().stageId();
    }

    int partitionId() {
      return TaskContext.get().partitionId();
    }

    long uniqueTaskId() {
      return TaskContext.get().taskAttemptId();
    }

    Properties tableProperties() {
      return tableProperties;
    }

    Deserializer newTableDeserializer(Configuration conf) {
      return HiveUtil.newTableDeserializer(tableDeserializerClass, tableProperties(), conf);
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition split) {
      return new InputFormatReader((InputFormatReadTask) split, this);
    }
  }

  private static class InputFormatReader extends TaskReader<InternalRow> {
    private final InputFormatReadTask split;
    private final InputFormatReaderFactory config;
    private final JobConf conf;
    private final Deserializer tableDeserializer;
    private final SpecificInternalRow reusedRow;

    private InputFormatReader(InputFormatReadTask split, InputFormatReaderFactory config) {
      super(split.readTask());
      this.split = split;
      this.config = config;
      TaskAttemptContext taskContext = MapReduceUtil.newTaskContext(
          config.conf(), config.batchId(), config.stageId(), config.partitionId(), config.uniqueTaskId());
      this.conf = new JobConf(taskContext.getConfiguration());
      this.tableDeserializer = config.newTableDeserializer(conf);
      this.reusedRow = new SpecificInternalRow(config.dataSchema());
    }

    RecordReader<?, Writable> openReader(PartitionedFile file, PartitionInfo info) {
      InputFormat<Writable, Writable> inputFormat = info.inputFormat(conf);
      FileInputFormat.setInputPaths(conf, new Path(file.filePath()));
      FileSplit fileSplit = new FileSplit(new Path(file.filePath()), file.start(), file.length(), NO_LOCALITY);
      try {
        return inputFormat.getRecordReader(fileSplit, conf, Reporter.NULL);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to open file: %s", file.filePath());
      }
    }

    @Override
    Iterator<InternalRow> open(PartitionedFile file) {
      PartitionInfo info = split.info(file.partitionValues());

      Iterator<Writable> iter = new RecordReaderValueIterator<>(openReader(file, info));
      scala.collection.Iterator<InternalRow> scalaIter = HadoopTableReader.fillObject(
          ScalaUtil.asScala(iter), info.newDeserializer(config.tableProperties(), conf), attrs(config.dataSchema()),
          reusedRow, tableDeserializer);

      Iterator<InternalRow> dataIter = ScalaUtil.asJava(scalaIter);
      if (file.partitionValues().numFields() > 0) {
        JoinedRow joined = new JoinedRow();
        joined.withRight(file.partitionValues());
        return Iterators.transform(dataIter, joined::withLeft);
      } else {
        return dataIter;
      }
    }

    private static Seq<Tuple2<Attribute, Object>> attrs(StructType struct) {
      List<Tuple2<Attribute, Object>> attrs = Lists.newArrayList();
      StructField[] fields = struct.fields();
      for (int i = 0; i < fields.length; i += 1) {
        StructField field = fields[i];
        attrs.add(Tuple2.apply(new AttributeReference(
            field.name(), field.dataType(), field.nullable(), field.metadata(), ExprId.apply(i), ScalaUtil.nil()), i));
      }
      return ScalaUtil.asScala(attrs);
    }
  }

  private static class RecordReaderValueIterator<K, V> implements Iterator<V>, Closeable {
    private final RecordReader<K, V> reader;
    private boolean hasNext = false;
    private boolean consumed = true;
    private K key;
    private V value;

    private RecordReaderValueIterator(RecordReader<K, V> reader) {
      this.reader = reader;
      this.key = reader.createKey();
      this.value = reader.createValue();
    }

    @Override
    public boolean hasNext() {
      if (consumed) {
        this.consumed = false;
        this.hasNext = false;
        try {
          this.hasNext = reader.next(key, value);
        } catch (IOException e) {
          throw new RuntimeIOException(e, "Failed to read");
        }
      }

      return hasNext;
    }

    @Override
    public V next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      this.consumed = true;

      return value;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  private static String key(StructType type, InternalRow row) {
    return IntStream.range(0, row.numFields()).boxed()
        .map(pos -> row.isNullAt(pos) ? "null" : row.get(pos, type.fields()[pos].dataType()))
        .map(String::valueOf)
        .collect(Collectors.joining("/"));
  }

}
