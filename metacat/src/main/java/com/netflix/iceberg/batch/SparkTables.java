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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.netflix.metacat.common.dto.PartitionDto;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.AvroFileFormat;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat;
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.hive.orc.OrcFileFormat;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class SparkTables {
  private static final Joiner DOT = Joiner.on('.');
  private static final int SCHEMA_SPLIT_LENGTH = 4000;
  private static final String SPARK_PROPERTY_PREFIX = "spark.sql.sources.";
  private static final String OPTION_PREFIX = "option.";
  private static final String PROVIDER_PROP = "spark.sql.sources.provider";
  private static final String SCHEMA_PROP = "spark.sql.sources.schema";
  private static final String SCHEMA_PARTS_PROP = "spark.sql.sources.schema.numParts";
  private static final String SCHEMA_PARTS_PREFIX = "spark.sql.sources.schema.part.";
  private static final String PARTITION_PARTS_PROP = "spark.sql.sources.schema.numPartCols";
  private static final String PARTITION_PARTS_PREFIX = "spark.sql.sources.schema.partCol.";
  private static final String BUCKET_PARTS_PROP = "spark.sql.sources.schema.numBucketCols";
  private static final String FORMAT = "format";
  private static final String PARTITION_BY_NAME = "spark.behavior.partition-by-name";
  private static final String BEHAVIOR_COMPAT = "spark.behavior.compatibility";
  static final String HIVE_STORED_AS = "hive.stored-as";
  static final String HIVE_INPUT_FORMAT = "hive.input-format";
  static final String HIVE_OUTPUT_FORMAT = "hive.output-format";
  static final String HIVE_SERDE = "hive.serde";
  static final String LOCATION = "location";
  private static final String PROVIDER = "provider";
  private static final String PATH = "path";
  private static final Set<String> RESERVED_OPTIONS = Sets.newHashSet(PATH);
  private static final Set<String> RESERVED_PROPERTIES = Sets.newHashSet(
      LOCATION, PROVIDER, PATH, FORMAT, PARTITION_BY_NAME, BEHAVIOR_COMPAT,
      HIVE_STORED_AS, HIVE_INPUT_FORMAT, HIVE_OUTPUT_FORMAT, HIVE_SERDE);
  private static final Set<String> PARQUET_PROVIDERS = Sets.newHashSet("hive", "parquet", "hive_parquet");
  private static final Set<String> AVRO_PROVIDERS = Sets.newHashSet("avro", "hive_avro");
  private static final Set<String> ORC_PROVIDERS = Sets.newHashSet("orc", "hive_orc");
  private static final Set<String> HIVE_PROVIDERS = Sets.newHashSet("orc", "avro", "parquet");

  static Configuration buildConf(SparkSession spark, Map<String, String> options) {
    // this is equivalent to Spark's newHadoopConfWithOptions
    Configuration conf = spark.sessionState().newHadoopConf();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      if (!entry.getKey().equalsIgnoreCase("path") && !entry.getKey().equalsIgnoreCase("paths")) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }

    return conf;
  }

  static boolean isReservedProperty(String property) {
    return RESERVED_PROPERTIES.contains(property) ||
        property.startsWith(SPARK_PROPERTY_PREFIX) ||
        property.startsWith(OPTION_PREFIX);
  }

  static Map<String, String> properties(Map<String, String> properties, String provider,
                                        String inputFormat, String outputFormat, String serde) {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    if (provider == null || HIVE_PROVIDERS.contains(provider)) {
      // these providers use real Hive input formats
      if (provider != null) {
        propsBuilder.put(FORMAT, "hive/" + provider);
      } else {
        propsBuilder.put(FORMAT, "hive/inputformat");
      }

      if (inputFormat != null) {
        propsBuilder.put(HIVE_INPUT_FORMAT, inputFormat);
      }

      if (outputFormat != null) {
        propsBuilder.put(HIVE_OUTPUT_FORMAT, outputFormat);
      }

      if (serde != null) {
        propsBuilder.put(HIVE_SERDE, serde);
      }
    } else {
      propsBuilder.put(FORMAT, "spark/" + provider);
      propsBuilder.put(PROVIDER, provider);
    }

    propsBuilder.put(PARTITION_BY_NAME, "true");
    propsBuilder.put(BEHAVIOR_COMPAT, "true");

    properties.entrySet().stream()
        .filter(entry -> !SparkTables.isReservedProperty(entry.getKey()))
        .forEach(propsBuilder::put);
    return propsBuilder.build();
  }

  static Map<String, String> options(Map<String, String> properties) {
    return options(properties, null);
  }

  static Map<String, String> options(Map<String, String> properties, String location) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    if (location != null) {
      builder.put(PATH, location);
    }

    removeKeyPrefix(properties, OPTION_PREFIX).entrySet().stream()
        .filter(entry -> !RESERVED_OPTIONS.contains(entry.getKey()))
        .forEach(builder::put);

    return builder.build();
  }

  static Map<String, String> updateOptions(Map<String, String> options, String location,
                                           Map<String, String> newProperties, Set<String> removedProperties) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    if (location != null && options.containsKey(PATH)) {
      builder.put(PATH, location);
    }

    Map<String, String> newOptions = removeKeyPrefix(newProperties, OPTION_PREFIX);
    Set<String> removedOptions = removePrefix(removedProperties, OPTION_PREFIX);

    options.entrySet().stream()
        .filter(entry -> !RESERVED_OPTIONS.contains(entry.getKey()))
        .filter(entry -> !removedOptions.contains(entry.getKey()))
        .filter(entry -> !newOptions.containsKey(entry.getKey()))
        .forEach(builder::put);

    newOptions.entrySet().stream()
        .filter(entry -> !RESERVED_OPTIONS.contains(entry.getKey()))
        .forEach(builder::put);

    return builder.build();
  }

  static Transform[] partitionTransforms(Map<String, String> properties) {
    Preconditions.checkArgument(
        PropertyUtil.propertyAsInt(properties, BUCKET_PARTS_PROP, 0) < 1,
        "Cannot read table: bucketing is not supported");

    if (!properties.containsKey(PARTITION_PARTS_PROP)) {
      return null;
    }

    int numPartitions = PropertyUtil.propertyAsInt(properties, PARTITION_PARTS_PROP, 0);
    List<String> partitionColumns = Lists.newArrayList();
    for (int i = 0; i < numPartitions; i += 1) {
      partitionColumns.add(properties.get(PARTITION_PARTS_PREFIX + i));
    }

    return partitionColumns.stream()
        .map(Expressions::identity)
        .toArray(Transform[]::new);
  }

  @SuppressWarnings("unchecked")
  static StructType schema(Map<String, String> properties) {
    // if the provider is not set, the Spark properties should be ignored
    if (properties.get(PROVIDER_PROP) == null) {
      return null;
    }

    if (properties.containsKey(SCHEMA_PROP)) {
      return (StructType) DataType.fromJson(properties.get(SCHEMA_PROP));

    } else if (properties.containsKey(SCHEMA_PARTS_PROP)) {
      StringBuilder sb = new StringBuilder();
      int numParts = PropertyUtil.propertyAsInt(properties, SCHEMA_PARTS_PROP, 0);
      for (int i = 0; i < numParts; i += 1) {
        sb.append(properties.get(SCHEMA_PARTS_PREFIX + i));
      }
      return (StructType) DataType.fromJson(sb.toString());
    }

    return null;
  }

  static String provider(Map<String, String> properties) {
    String provider = properties.get(PROVIDER);
    String storedAs = properties.get(HIVE_STORED_AS);
    String inputFormat = properties.get(HIVE_INPUT_FORMAT);

    if (provider == null || "hive".equalsIgnoreCase(provider)) {
      // provider is defaulted or not set
      return provider(storedAs, inputFormat);
    }

    // provider was set and is not a default value
    Preconditions.checkArgument(
        storedAs == null || storedAs.equalsIgnoreCase(provider),
        "Cannot use both STORED AS %s and USING %s", storedAs, provider);
    Preconditions.checkArgument(
        inputFormat == null,
        "Cannot use both INPUTFORMAT/OUTPUTFORMAT %s and USING %s", inputFormat, provider);

    return provider(provider, null);
  }

  static String provider(Map<String, String> properties, String inputFormat) {
    return provider(properties.get(PROVIDER_PROP), inputFormat);
  }

  private static String provider(String provider, String inputFormat) {
    // TODO: figure out how to handle JDBC
    String lowerCaseProvider = provider != null ? provider.toLowerCase(Locale.ROOT) : null;
    if (PARQUET_PROVIDERS.contains(lowerCaseProvider) || contains(inputFormat, "parquet")) {
      return "parquet";
    } else if (ORC_PROVIDERS.contains(lowerCaseProvider) || contains(inputFormat, "orc")) {
      return "orc";
    } else if (AVRO_PROVIDERS.contains(lowerCaseProvider) || contains(inputFormat, "avro")) {
      return "avro";
    } else if ("jdbc".equals(lowerCaseProvider)) {
      throw new UnsupportedOperationException("Unsupported format: JDBC");
    } else if ("csv".equals(lowerCaseProvider)) {
      return "csv";
    } else if ("json".equals(lowerCaseProvider)) {
      return "json";
    }

    return null;
  }

  private static boolean contains(String maybeContains, String substr) {
    if (maybeContains != null) {
      return maybeContains.contains(substr);
    }
    return false;
  }

  private static final Set<String> SUPPORTED_PROVIDERS = Sets.newHashSet("parquet", "orc", "avro", "csv", "json");

  static boolean isSupportedFormat(String provider) {
    if (provider != null) {
      return SUPPORTED_PROVIDERS.contains(provider.toLowerCase(Locale.ROOT));
    }
    return false;
  }

  static FileFormat format(String provider) {
    if (provider == null) {
      return null;
    }

    switch (provider.toLowerCase(Locale.ROOT)) {
      case "parquet":
        return new ParquetFileFormat();
      case "orc":
        return new OrcFileFormat();
      case "avro":
        return new AvroFileFormat();
      case "csv":
        return new CSVFileFormat();
      case "json":
        return new JsonFileFormat();
    }

    throw new UnsupportedOperationException(String.format("Unsupported file format: %s", provider));
  }

  static String serde(String provider) {
    if ("parquet".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    } else if ("orc".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
    } else if ("avro".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
    } else {
      return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    }
  }

  static String inputFormat(String provider) {
    if ("parquet".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    } else if ("orc".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    } else if ("avro".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
    } else {
      return "org.apache.hadoop.mapred.SequenceFileInputFormat";
    }
  }

  static String outputFormat(String provider) {
    if ("parquet".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    } else if ("orc".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    } else if ("avro".equalsIgnoreCase(provider)) {
      return "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
    } else {
      return "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat";
    }
  }

  static String defaultInputFormat(String storedAs) {
    if ("textfile".equalsIgnoreCase(storedAs) || "text".equalsIgnoreCase(storedAs)) {
      return "org.apache.hadoop.mapred.TextInputFormat";
    } else if ("sequencefile".equalsIgnoreCase(storedAs) || "sequence".equalsIgnoreCase(storedAs)) {
      return "org.apache.hadoop.mapred.SequenceFileInputFormat";
    } else {
      throw new UnsupportedOperationException("Unknown STORED AS format: " + storedAs);
    }
  }

  static String defaultOutputFormat(String storedAs) {
    if ("textfile".equalsIgnoreCase(storedAs)) {
      return "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    } else if ("sequencefile".equalsIgnoreCase(storedAs)) {
      return "org.apache.hadoop.mapred.SequenceFileOutputFormat";
    } else {
      throw new UnsupportedOperationException("Unknown STORED AS format: " + storedAs);
    }
  }

  static String defaultSerde(String storedAs) {
    return "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  }

  private static final Set<String> TESTABLE_FORMAT_PROVIDERS = Sets.newHashSet("parquet", "avro", "orc");

  static boolean canRead(String provider, PartitionDto partition) {
    // if the provider input format can't be tested, assume it matches. otherwise, check the partition input format
    return !TESTABLE_FORMAT_PROVIDERS.contains(provider) || partition.getSerde().getInputFormat().contains(provider);
  }

  static Map<String, String> toSparkProperties(StructType schema, Transform[] transforms,
                                               Map<String, String> properties) {
    // use the provider from the incoming properties, without defaulting it
    // this avoids accidentally converting Hive tables to Spark tables
    return toSparkProperties(schema, transforms, properties.get(PROVIDER_PROP));
  }

  static Map<String, String> toSparkProperties(StructType schema, Transform[] transforms, String provider) {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    List<String> schemaParts = split(schema.json());
    propsBuilder.put(SCHEMA_PARTS_PROP, String.valueOf(schemaParts.size()));
    for (int i = 0; i < schemaParts.size(); i += 1) {
      propsBuilder.put(SCHEMA_PARTS_PREFIX + i, schemaParts.get(i));
    }

    propsBuilder.put(PARTITION_PARTS_PROP, String.valueOf(transforms.length));
    for (int i = 0; i < transforms.length; i += 1) {
      propsBuilder.put(PARTITION_PARTS_PREFIX + i, extractFieldName(transforms[i]));
    }

    if (provider != null) {
      propsBuilder.put(PROVIDER_PROP, provider);
    }

    return propsBuilder.build();
  }

  private static List<String> split(String str) {
    List<String> parts = Lists.newArrayList();

    int offset = 0;
    int remaining = str.length();
    while (remaining > 0) {
      int len = Math.min(remaining, SCHEMA_SPLIT_LENGTH);
      parts.add(str.substring(offset, offset + len));
      remaining -= len;
      offset += len;
    }

    return parts;
  }

  static StructType expectedProjection(StructType requestedSchema, StructType partitionSchema) {
    Set<String> partitionFieldNames = Sets.newHashSet(partitionSchema.fieldNames());
    StructField[] fields = Streams.concat(
        Stream.of(requestedSchema.fields()).filter(field -> !partitionFieldNames.contains(field.name())),
        Stream.of(partitionSchema.fields())
    ).toArray(StructField[]::new);
    return new StructType(fields);
  }

  static StructType dataProjection(StructType expectedSchema, StructType partitionSchema) {
    Set<String> partitionFieldNames = Sets.newHashSet(partitionSchema.fieldNames());
    StructField[] dataFields = Stream.of(expectedSchema.fields())
        .filter(field -> !partitionFieldNames.contains(field.name()))
        .toArray(StructField[]::new);
    return new StructType(dataFields);
  }

  static StructType partitionProjection(StructType expectedSchema, StructType partitionSchema) {
    Set<String> partitionFieldNames = Sets.newHashSet(partitionSchema.fieldNames());
    StructField[] dataFields = Stream.of(expectedSchema.fields())
        .filter(field -> partitionFieldNames.contains(field.name()))
        .toArray(StructField[]::new);
    return new StructType(dataFields);
  }

  static Set<String> partitionFields(Transform[] transforms) {
    return Stream.of(transforms)
        .map(SparkTables::extractFieldName)
        .collect(Collectors.toSet());
  }

  static StructType applySchemaChanges(MetacatSparkTable table, TableChange... changes) {
    StructType struct = table.schema();
    Set<String> partitionColumnNames = partitionFields(table.partitioning());

    // keep track of additions in newColumns and updates in the columns map
    List<StructField> newColumns = Lists.newArrayList();
    Map<String, StructField> columns = Maps.newHashMap();
    Stream.of(struct.fields()).forEach(column -> columns.put(column.name(), column));

    for (TableChange change : changes) {
      if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn add = (TableChange.AddColumn) change;
        StructField field = new StructField(name(add), add.dataType(), true, Metadata.empty());
        newColumns.add(Optional.ofNullable(add.comment()).map(field::withComment).orElse(field));

      } else if (change instanceof TableChange.UpdateColumnComment) {
        TableChange.UpdateColumnComment update = (TableChange.UpdateColumnComment) change;
        String name = name(update);
        StructField field = columns.get(name);
        Preconditions.checkArgument(field != null, "Cannot update column comment, missing field: %s", name);
        columns.put(name, field.withComment(update.newComment()));

      } else if (change instanceof TableChange.UpdateColumnType) {
        throw new UnsupportedOperationException("Updating the type of a column is not supported: "
            + DOT.join(((TableChange.UpdateColumnType) change).fieldNames()));

      } else if (change instanceof TableChange.DeleteColumn) {
        throw new UnsupportedOperationException("Deleting a column is not supported: "
            + DOT.join(((TableChange.DeleteColumn) change).fieldNames()));

      } else if (change instanceof TableChange.RenameColumn) {
        throw new UnsupportedOperationException("Renaming a column is not supported: "
            + DOT.join(((TableChange.RenameColumn) change).fieldNames()));

      } else if (change instanceof TableChange.UpdateColumnNullability) {
        throw new UnsupportedOperationException("Updating the nullability of a column is not supported: "
            + DOT.join(((TableChange.UpdateColumnNullability) change).fieldNames()));

      } else {
        throw new IllegalArgumentException("Invalid schema change: " + change);
      }
    }

    // replace existing columns by name with columns in the columns map to pick up comment changes
    Stream<StructField> dataColumns = Stream.of(struct.fields())
        .map(StructField::name)
        .filter(((Predicate<String>) partitionColumnNames::contains).negate())
        .map(columns::get);
    Stream<StructField> partitionColumns = Stream.of(struct.fields())
        .map(StructField::name)
        .filter(partitionColumnNames::contains)
        .map(columns::get);

    // create a new schema out of non-partition columns, then new columns, then partition columns.
    return new StructType(
        Streams.concat(dataColumns, newColumns.stream(), partitionColumns).toArray(StructField[]::new));
  }

  static FileAppender<InternalRow> asAppender(OutputWriter writer) {
    return new OutputWriterAppender(writer);
  }

  private static class OutputWriterAppender implements FileAppender<InternalRow> {
    private final OutputWriter writer;

    private OutputWriterAppender(OutputWriter writer) {
      this.writer = writer;
    }

    @Override
    public void add(InternalRow row) {
      writer.write(row);
    }

    @Override
    public Metrics metrics() {
      return null;
    }

    @Override
    public long length() {
      return 0;
    }

    @Override
    public void close() {
      writer.close();
    }
  }

  private static String name(TableChange.UpdateColumnComment update) {
    Preconditions.checkArgument(update.fieldNames().length == 1,
        "Cannot update column comment, nesting is not supported: %s", DOT.join(update.fieldNames()));
    return update.fieldNames()[0];
  }

  private static String name(TableChange.AddColumn add) {
    Preconditions.checkArgument(add.fieldNames().length == 1,
        "Cannot add column, nesting is not supported: %s", DOT.join(add.fieldNames()));
    return add.fieldNames()[0];
  }

  private static String extractFieldName(Transform transform) {
    ValidationException.check("identity".equalsIgnoreCase(transform.name()),
        "Cannot partition non-Iceberg table using %s", transform);
    ValidationException.check(transform.references().length == 1,
        "Cannot partition using multiple field references: %s", transform);
    NamedReference ref = transform.references()[0];

    ValidationException.check(ref.fieldNames().length == 1,
        "Cannot partition non-Iceberg table with a nested field: %s", transform);
    return ref.fieldNames()[0];
  }

  private static Map<String, String> removeKeyPrefix(Map<String, String> map, String prefix) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    map.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(prefix))
        .forEach(entry -> builder.put(entry.getKey().replaceFirst(prefix, ""), entry.getKey()));
    return builder.build();
  }

  private static Set<String> removePrefix(Set<String> set, String prefix) {
    return set.stream()
        .filter(str -> str.startsWith(prefix))
        .map(str -> str.replaceFirst(prefix, ""))
        .collect(Collectors.toSet());
  }
}
