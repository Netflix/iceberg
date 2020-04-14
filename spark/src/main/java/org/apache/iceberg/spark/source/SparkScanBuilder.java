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
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;

public class SparkScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
  private static final Filter[] NO_FILTERS = new Filter[0];
  private static final String[] NO_META_COLUMNS = new String[0];

  private final SparkSession spark;
  private final Table table;
  private final CaseInsensitiveStringMap options;

  private Schema schema;
  private String[] metaColumns = NO_META_COLUMNS;
  private boolean caseSensitive;
  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = NO_FILTERS;
  private Long snapshotId = null;

  SparkScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this.spark = spark;
    this.table = table;
    this.options = options;
    this.schema = table.schema();
    this.caseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive"));
  }

  public SparkScanBuilder caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  public SparkScanBuilder snapshotId(long snapshotId) {
    Preconditions.checkState(table.snapshot(snapshotId) != null, "Cannot find snapshot with ID: %s", snapshotId);
    this.snapshotId = snapshotId;
    return this;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        try {
          Binder.bind(table.schema().asStruct(), expr, caseSensitive);
          expressions.add(expr);
          pushed.add(filter);
        } catch (ValidationException e) {
          // binding to the table schema failed, so this expression cannot be pushed down
        }
      }
    }

    this.filterExpressions = expressions;
    this.pushedFilters = pushed.toArray(new Filter[0]);

    // Spark doesn't support residuals per task, so return all filters
    // to get Spark to handle record-level filtering
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requestedSchema) {
    StructType requestedWithoutMetadataColumns = new StructType(Stream.of(requestedSchema.fields())
        .filter(field -> MetadataColumns.nonMetadataColumn(field.name()))
        .toArray(StructField[]::new));

    this.schema = SparkSchemaUtil.prune(table.schema(), requestedWithoutMetadataColumns);
    this.metaColumns = Stream.of(requestedSchema.fields())
        .map(StructField::name)
        .filter(MetadataColumns::isMetadataColumn)
        .distinct()
        .toArray(String[]::new);
  }

  @Override
  public Scan build() {
    // Pass snapshot ID using read options to avoid changing the constructor args
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options.asCaseSensitiveMap());
    if (snapshotId != null) {
      builder.put("snapshot-id", String.valueOf(snapshotId));
    }
    String targetSize = envSplitSize();
    if (targetSize != null) {
      builder.put("split-size", targetSize);
    }
    CaseInsensitiveStringMap newOptions = new CaseInsensitiveStringMap(builder.build());

    return new SparkBatchScan(table, caseSensitive, schema, metaColumns, filterExpressions, newOptions);
  }

  private String envSplitSize() {
    String[] names = table.toString().split("\\.", 2);
    if (names.length > 1) {
      String envTargetSizeProp = String.format("spark.netflix.%s.target-size", names[1]);
      Option<String> prop = spark.conf().getOption(envTargetSizeProp);
      if (prop.isDefined()) {
        return prop.get();
      }
    }
    return null;
  }
}
