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

import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import parquet.Preconditions;

// TODO: implement SupportsOverwrite. expression filters from Spark should always align with partitions.

class BatchPatternWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsTruncate {
  private final SparkSession spark;
  private final MetacatSparkTable table;
  private final Map<String, String> options;
  private String writeUUID = null;
  private StructType schema;
  private boolean overwriteDynamic = false;

  BatchPatternWriteBuilder(SparkSession spark, MetacatSparkTable table, CaseInsensitiveStringMap options) {
    this.spark = spark;
    this.table = table;
    this.options = options.asCaseSensitiveMap();
  }

  @Override
  public WriteBuilder withQueryId(String queryId) {
    this.writeUUID = queryId;
    return this;
  }

  @Override
  public WriteBuilder withInputDataSchema(StructType schema) {
    this.schema = schema;
    return this;
  }

  @Override
  public WriteBuilder truncate() {
    Preconditions.checkState(table.partitioning().length == 0,
        "Truncate is not supported for partitioned table: %s", table);
    // when the table is not partitioned, dynamic overwrite and truncate are the same
    return overwriteDynamicPartitions();
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    this.overwriteDynamic = true;
    return this;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new BatchPatternWrite(spark, table, writeUUID, schema, overwriteDynamic, options);
  }
}
