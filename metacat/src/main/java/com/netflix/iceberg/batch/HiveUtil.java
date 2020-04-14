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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.hive.HadoopTableReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveUtil {
  private HiveUtil() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(HiveUtil.class);
  private static final Joiner DOT = Joiner.on('.');

  static <C> Class<? extends C> loadClass(String className, Class<C> superClass) {
    return DynClasses.builder().impl(className).build().asSubclass(superClass);
  }

  static <S, C extends S> C newInstance(String className, Class<S> superClass, Configuration conf) {
    return newInstance(loadClass(className, superClass), conf);
  }

  static <C> C newInstance(Class<C> theClass, Configuration conf) {
    try {
      C instance = theClass.newInstance();
      if (conf != null && instance instanceof Configurable) {
        ((Configurable) instance).setConf(conf);
      }
      return instance;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Failed to instantiate input format: " + theClass.getName(), e);
    }
  }

  static Deserializer newTableDeserializer(String tableDeserializerClass, Properties tableProperties,
                                           Configuration conf) {
    Deserializer deserializer = HiveUtil.newInstance(tableDeserializerClass, Deserializer.class, conf);

    try {
      if (deserializer instanceof AbstractSerDe) {
        ((AbstractSerDe) deserializer).initialize(conf, tableProperties, null);
        String errorMessage = ((AbstractSerDe) deserializer).getConfigurationErrors();
        if (errorMessage != null && !errorMessage.isEmpty()) {
          LOG.warn("Error initializing deserializer: {}", errorMessage);
        }
      } else {
        deserializer.initialize(conf, tableProperties);
      }
    } catch (SerDeException e) {
      throw new RuntimeException("Failed to initialize deserializer: " + tableDeserializerClass);
    }

    return deserializer;
  }

  static Deserializer newDeserializer(String partitionDeserializerClass, Properties tableProperties,
                                      Properties partitionProperties, Configuration conf) {
    Deserializer deserializer = HiveUtil.newInstance(partitionDeserializerClass, Deserializer.class, conf);

    try {
      if (deserializer instanceof AbstractSerDe) {
        ((AbstractSerDe) deserializer).initialize(conf, tableProperties, partitionProperties);
        String errorMessage = ((AbstractSerDe) deserializer).getConfigurationErrors();
        if (errorMessage != null && !errorMessage.isEmpty()) {
          LOG.warn("Error initializing deserializer: {}", errorMessage);
        }
      } else {
        Properties combinedProperties = new Properties(tableProperties);
        if (partitionProperties != null) {
          combinedProperties.putAll(partitionProperties);
        }
        deserializer.initialize(conf, combinedProperties);
      }
    } catch (SerDeException e) {
      throw new RuntimeException("Failed to initialize deserializer: " + partitionDeserializerClass);
    }

    return deserializer;
  }

  static Serializer newTableSerializer(String tableSerializerClass, Properties tableProperties, Configuration conf) {
    Serializer serializer = HiveUtil.newInstance(tableSerializerClass, Serializer.class, conf);

    try {
      if (serializer instanceof AbstractSerDe) {
        ((AbstractSerDe) serializer).initialize(conf, tableProperties, null);
        String errorMessage = ((AbstractSerDe) serializer).getConfigurationErrors();
        if (errorMessage != null && !errorMessage.isEmpty()) {
          LOG.warn("Error initializing serializer: {}", errorMessage);
        }
      } else {
        serializer.initialize(conf, tableProperties);
      }
    } catch (SerDeException e) {
      throw new RuntimeException("Failed to initialize serializer: " + tableSerializerClass);
    }

    return serializer;
  }

  // This was translated to Java from Spark's HiveFileFormat. Its behavior should match what Spark uses.
  static class HiveOutputWriterFactory extends OutputWriterFactory {
    private final Broadcast<SerializableConfiguration> confBroadcast;
    private final String location;
    private final String serializerClass;
    private final TableDesc tableDesc;

    // lazy data
    private transient JobConf jobConf = null;
    private transient FileSinkDesc sinkDesc = null;
    private transient Serializer serializer = null;
    private transient StructObjectInspector structInspector = null;
    private transient ObjectInspector[] fieldInspectors = null;
    private transient OutputFormat outputFormat = null;

    public HiveOutputWriterFactory(Broadcast<SerializableConfiguration> confBroadcast, TableDto table) {
      this.confBroadcast = confBroadcast;
      this.location = table.getSerde().getUri();
      this.serializerClass = table.getSerde().getSerializationLib();
      this.tableDesc = new TableDesc(
          loadClass(table.getSerde().getInputFormat(), InputFormat.class),
          loadClass(table.getSerde().getOutputFormat(), OutputFormat.class),
          buildTableProperties(table));
    }

    private JobConf jobConf() {
      if (jobConf == null) {
        this.jobConf = new JobConf(confBroadcast.getValue().value());
      }
      return jobConf;
    }

    private TableDesc tableDesc() {
      return tableDesc;
    }

    private FileSinkDesc sinkDesc() {
      if (sinkDesc == null) {
        this.sinkDesc = new FileSinkDesc(new Path(location), tableDesc, true);
        sinkDesc.setCompressed(jobConf().getBoolean("hive.exec.compress.output", false));
        sinkDesc.setCompressCodec(jobConf().get("mapreduce.output.fileoutputformat.compress.codec"));
        sinkDesc.setCompressType(jobConf().get("mapreduce.output.fileoutputformat.compress.type"));
      }
      return sinkDesc;
    }

    private Deserializer deserializer() {
      Preconditions.checkState(serializer() instanceof Deserializer,
          "Invalid serde class (not Deserializer): " + serializerClass);
      return (Deserializer) serializer();
    }

    private Serializer serializer() {
      if (serializer == null) {
        this.serializer = newTableSerializer(serializerClass, tableDesc.getProperties(), jobConf());
      }
      return serializer;
    }

    private StructObjectInspector structInspector() {
      if (structInspector == null) {
        try {
          structInspector = (StructObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(
              deserializer().getObjectInspector(),
              ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
        } catch (SerDeException e) {
          throw new RuntimeException(e);
        }
      }
      return structInspector;
    }

    private ObjectInspector[] fieldInspectors() {
      if (fieldInspectors == null) {
        this.fieldInspectors = structInspector().getAllStructFieldRefs().stream()
            .map(StructField::getFieldObjectInspector)
            .toArray(ObjectInspector[]::new);
      }
      return fieldInspectors;
    }

    private OutputFormat<?, ?> outputFormat() {
      if (outputFormat == null) {
        this.outputFormat = HiveUtil.newInstance(tableDesc.getOutputFileFormatClass(), jobConf());
      }
      return outputFormat;
    }

    private HiveOutputFormat<?, ?> hiveOutputFormat() {
      if (outputFormat() instanceof HiveOutputFormat) {
        return (HiveOutputFormat<?, ?>) outputFormat();
      }
      return null;
    }

    @Override
    public String getFileExtension(TaskAttemptContext context) {
      return Utilities.getFileExtension(jobConf(), sinkDesc().getCompressed(), hiveOutputFormat());
    }

    @Override
    public OutputWriter newInstance(String path, StructType dataSchema, TaskAttemptContext context) {
      return new HiveOutputWriter(path, dataSchema, this);
    }

    private static class HiveOutputWriter extends OutputWriter {
      private final DataType[] dataTypes;
      private final ObjectInspector structInspector;
      private final Function<Object, Object>[] wrappers;
      private final Serializer serializer;
      private final Object[] outputData;
      private final FileSinkOperator.RecordWriter hiveWriter;

      public HiveOutputWriter(String path, StructType dataSchema, HiveOutputWriterFactory config) {
        this.dataTypes = Stream.of(dataSchema.fields())
            .map(org.apache.spark.sql.types.StructField::dataType)
            .toArray(DataType[]::new);
        this.structInspector = config.structInspector();
        this.wrappers = convertersFor(config.fieldInspectors(), dataTypes);
        this.serializer = config.serializer();
        this.outputData = new Object[dataTypes.length];

        try {
          this.hiveWriter = HiveFileFormatUtils.getHiveRecordWriter(
              config.jobConf(),
              config.tableDesc(),
              serializer.getSerializedClass(),
              config.sinkDesc(),
              new Path(path),
              Reporter.NULL);
        } catch (HiveException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void write(InternalRow row) {
        for (int i = 0; i < outputData.length; i += 1) {
          if (row.isNullAt(i)) {
            outputData[i] = null;
          } else {
            outputData[i] = wrappers[i].apply(row.get(i, dataTypes[i]));
          }
        }

        try {
          hiveWriter.write(serializer.serialize(outputData, structInspector));
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        } catch (SerDeException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void close() {
        try {
          hiveWriter.close(false); // abort boolean does not matter
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }
    }
  }

  static Properties buildPartitionProperties(Properties tableProperties, PartitionDto partition) {
    return HiveUtil.setSerdeProperties(
        new Properties(tableProperties), partition.getSerde(), partition.getMetadata());
  }

  static Properties buildTableProperties(TableDto table) {
    String tableName = DOT.join(table.getName().getDatabaseName(), table.getName().getTableName());

    // In TableReader.makeRDDForPartitionedTable, Spark directly instantiates the Deserializer and initializes it
    // using only the table properties or partition properties. the extra properties here are probably not needed.
    // However, Spark only supports reading from one format so this is an attempt to support multi-format tables.

    Properties tableProperties = new Properties();
    tableProperties.setProperty("name", tableName);
    tableProperties.setProperty("columns",
        table.getFields().stream().map(FieldDto::getName).collect(Collectors.joining(",")));
    tableProperties.setProperty("columns.types",
        table.getFields().stream().map(FieldDto::getType).collect(Collectors.joining(":")));
    tableProperties.setProperty("partition_columns",
        table.getFields().stream()
            .filter(FieldDto::isPartition_key)
            .map(FieldDto::getName)
            .collect(Collectors.joining("/")));
    tableProperties.setProperty("partition_columns.types",
        table.getFields().stream()
            .filter(FieldDto::isPartition_key)
            .map(FieldDto::getType)
            .collect(Collectors.joining(":")));
    // additional Hive properties that are probably not needed:
    // properties.setProperty("serialization.ddl", "struct { (thrift-type) col1, (thrift-type) col2 }");
    // properties.setProperty("columns.comments", "blah\u0000blah");

    setSerdeProperties(tableProperties, table.getSerde(), table.getMetadata());

    return tableProperties;
  }

  static Properties setSerdeProperties(Properties properties, StorageDto serde, Map<String, String> metadata) {
    properties.setProperty("file.inputformat", serde.getInputFormat());
    properties.setProperty("file.outputformat", serde.getOutputFormat());
    properties.setProperty("serialization.lib", serde.getSerializationLib());
    properties.setProperty("location", serde.getUri());
    if (serde.getSerdeInfoParameters() != null) {
      properties.putAll(serde.getSerdeInfoParameters());
    }
    if (metadata != null) {
      properties.putAll(metadata);
    }
    return properties;
  }

  @SuppressWarnings("unchecked")
  static Function<Object, Object>[] convertersFor(ObjectInspector[] fieldOIs, DataType[] dataTypes) {
    Preconditions.checkArgument(fieldOIs.length == dataTypes.length, "[BUG] Not the same number of fields and types");
    return IntStream.range(0, dataTypes.length).boxed()
        .map(i -> (Function<Object, Object>) HadoopTableReader.wrapperFor(fieldOIs[i], dataTypes[i])::apply)
        .toArray(Function[]::new);
  }
}
