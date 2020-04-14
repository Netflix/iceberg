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

package org.apache.iceberg.spark.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.CheckCompatibility;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.iceberg.spark.source.ValidationUtil.validateWriteSchema;
import static org.apache.iceberg.spark.source.ValidationUtil.validatePartitionTransforms;

class SparkWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsOverwrite {

  private final SparkSession spark;
  private final Table table;
  private final CaseInsensitiveStringMap options;
  private final String overwriteMode;
  private final boolean behaviorCompatibility;
  private boolean overwriteDynamic = false;
  private boolean overwriteByFilter = false;
  private Expression overwriteExpr = null;
  private String writeQueryId = null;
  private StructType writeSchema = null;

  SparkWriteBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this.spark = spark;
    this.table = table;
    this.behaviorCompatibility = PropertyUtil.propertyAsBoolean(
        table.properties(), "spark.behavior.compatibility", false);
    this.options = options;
    this.overwriteMode = options.containsKey("overwrite-mode") ?
        options.get("overwrite-mode").toLowerCase(Locale.ROOT) : null;
  }

  @Override
  public WriteBuilder withQueryId(String queryId) {
    this.writeQueryId = queryId;
    return this;
  }

  @Override
  public WriteBuilder withInputDataSchema(StructType schemaInput) {
    this.writeSchema = schemaInput;
    return this;
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    Preconditions.checkState(!overwriteByFilter, "Cannot overwrite dynamically and by filter: %s", overwriteExpr);
    this.overwriteDynamic = true;
    return this;
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    this.overwriteExpr = SparkFilters.convert(filters);
    if (overwriteExpr == Expressions.alwaysTrue() && "dynamic".equals(overwriteMode)) {
      // use the write option to override truncating the table. use dynamic overwrite instead.
      this.overwriteDynamic = true;
    } else {
      Preconditions.checkState(!overwriteDynamic, "Cannot overwrite dynamically and by filter: %s", overwriteExpr);
      this.overwriteByFilter = true;
    }
    return this;
  }

  @Override
  public BatchWrite buildForBatch() {
    if (!overwriteByFilter && behaviorCompatibility) {
      overwriteDynamicPartitions();
    }

    // Validate
    Schema dsSchema = SparkSchemaUtil.convert(table.schema(), writeSchema);
    validateWriteSchema(table.schema(), dsSchema, checkNullability(options));
    validatePartitionTransforms(table.spec());

    // Get application id
    String appId = spark.sparkContext().applicationId();

    // Get write-audit-publish id
    String wapId = spark.conf().get("spark.wap.id", null);

    // Pass genie ID using write options to avoid changing the constructor args
    String genieId = spark.sparkContext().hadoopConfiguration().get("genie.job.id");
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options.asCaseSensitiveMap());
    if (genieId != null) {
      builder.put("genie-id", genieId);
    }
    CaseInsensitiveStringMap newOptions = new CaseInsensitiveStringMap(builder.build());

    return new SparkBatchWrite(table,
        newOptions, overwriteDynamic, overwriteByFilter, overwriteExpr, appId, wapId, dsSchema);
  }

  @Override
  public StreamingWrite buildForStreaming() {
    if (!overwriteByFilter && behaviorCompatibility) {
      overwriteDynamicPartitions();
    }

    // Validate
    Schema dsSchema = SparkSchemaUtil.convert(table.schema(), writeSchema);
    validateWriteSchema(table.schema(), dsSchema, checkNullability(options));
    validatePartitionTransforms(table.spec());

    // Change to streaming write if it is just append
    Preconditions.checkState(!overwriteDynamic,
        "Unsupported streaming operation: dynamic partition overwrite");
    Preconditions.checkState(!overwriteByFilter || overwriteExpr == Expressions.alwaysTrue(),
        "Unsupported streaming operation: overwrite by filter: %s", overwriteExpr);

    // Get application id
    String appId = spark.sparkContext().applicationId();
    String wapId = spark.conf().get("spark.wap.id", null);

    // Pass genie ID using write options to avoid changing the constructor args
    String genieId = spark.sparkContext().hadoopConfiguration().get("genie.job.id");
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options.asCaseSensitiveMap());
    if (genieId != null) {
      builder.put("genie-id", genieId);
    }
    CaseInsensitiveStringMap newOptions = new CaseInsensitiveStringMap(builder.build());

    return new SparkStreamingWrite(table, newOptions, overwriteByFilter, writeQueryId, appId, wapId, dsSchema);
  }

  private boolean checkNullability(CaseInsensitiveStringMap options) {
    boolean sparkCheckNullability = Boolean.parseBoolean(spark.conf()
        .get("spark.sql.iceberg.check-nullability", "true"));
    boolean dataFrameCheckNullability = options.getBoolean("check-nullability", true);
    return sparkCheckNullability && dataFrameCheckNullability;
  }
}
