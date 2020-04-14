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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.dto.PartitionDto;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class MetacatScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
  private static final Filter[] NO_FILTERS = new Filter[0];

  private final SparkSession spark;
  private final MetacatSparkTable table;
  private final Map<String, String> scanOptions;
  private StructType expectedSchema;
  private boolean caseSensitive;
  private Filter[] pushedFilters = NO_FILTERS;
  private List<Expression> partFilters = Lists.newArrayList();

  MetacatScanBuilder(SparkSession spark, MetacatSparkTable table, CaseInsensitiveStringMap options) {
    this.spark = spark;
    this.table = table;
    this.scanOptions = options.asCaseSensitiveMap();
    this.expectedSchema = this.table.schema();
    this.caseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive"));
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    partFilters.clear();
    List<Filter> convertedFilters = Lists.newArrayList();
    Projections.ProjectionEvaluator projection = Projections.inclusive(table.spec(), caseSensitive);
    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null && expr != Expressions.alwaysTrue()) {
        convertedFilters.add(filter);

        Expression inclusive = projection.project(Binder.bind(table.icebergSchema().asStruct(), expr, caseSensitive));
        if (inclusive != null && inclusive != Expressions.alwaysTrue()) {
          partFilters.add(inclusive);
        }
      }
    }

    this.pushedFilters = convertedFilters.toArray(NO_FILTERS);

    // run all filters
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.expectedSchema = requiredSchema;
  }

  @Override
  public Scan build() {
    String provider = table.provider();

    List<PartitionDto> partitions =
        table.partitions(partFilters.stream().reduce(Expressions.alwaysTrue(), Expressions::and));
    List<PartitionDto> readable = partitions.stream()
        .filter(partition -> SparkTables.canRead(provider, partition))
        .collect(Collectors.toList());

    boolean forceFileFormatReader = table.database().equals("default");

    if (provider != null && (partitions.size() == readable.size() || forceFileFormatReader)) {
      return new FileFormatScan(spark, table, expectedSchema, pushedFilters, partFilters, readable, scanOptions);
    } else {
      return new InputFormatScan(spark, table, expectedSchema, partFilters, partitions, scanOptions);
    }
  }
}
