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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.netflix.dse.mds.PartitionMetrics;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.io.FileAppender;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetacatMetricsAppender implements FileAppender<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(MetacatMetricsAppender.class);
  private static final Set<String> NUMERIC_TYPES = ImmutableSet.of(
      "short", "int", "bigint", "float", "double", "date", "timestamp");

  private final FileAppender<InternalRow> wrappedAppender;
  private final String genieId;
  private final File dataLocation;
  private final FieldMetrics[] fieldMetrics;
  private long rowCount = 0L;

  static FileAppender<InternalRow> wrap(FileAppender<InternalRow> appender, StructType schema, String genieId,
                                        String location) {
    if (location.startsWith("file:/") || location.startsWith("/")) {
      File dataLocation = new File(location.replaceFirst("^file:", ""));
      return new MetacatMetricsAppender(appender, schema, genieId, dataLocation);
    } else {
      return appender;
    }
  }

  private MetacatMetricsAppender(FileAppender<InternalRow> appender, StructType schema, String genieId,
                                 File dataLocation) {
    this.wrappedAppender = appender;
    this.genieId = genieId;
    this.dataLocation = dataLocation;

    StructField[] fields = schema.fields();
    this.fieldMetrics = new FieldMetrics[fields.length];
    for (int i = 0; i < fields.length; i += 1) {
      fieldMetrics[i] = new FieldMetrics(i, fields[i]);
    }
  }

  @Override
  public void add(InternalRow row) {
    updateMetricsFromRow(row);
    wrappedAppender.add(row);
  }

  private void updateMetricsFromRow(InternalRow row) {
    rowCount += 1;
    for (FieldMetrics fieldMetric : fieldMetrics) {
      fieldMetric.update(row);
    }
  }

  @Override
  public Metrics metrics() {
    return wrappedAppender.metrics();
  }

  @Override
  public long length() {
    return wrappedAppender.length();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void close() throws IOException {
    wrappedAppender.close();

    if (dataLocation.exists()) {
      long totalSize = dataLocation.length();
      long nullCount = Stream.of(fieldMetrics).map(FieldMetrics::numNulls).reduce(0L, Long::sum);

      ImmutableMap.Builder<String, Long> nullCounts = ImmutableMap.builder();
      Stream.of(fieldMetrics).forEach(field -> nullCounts.put(field.name(), field.numNulls()));

      ImmutableMap.Builder<String, Double> minValues = ImmutableMap.builder();
      ImmutableMap.Builder<String, Double> maxValues = ImmutableMap.builder();
      Stream.of(fieldMetrics)
          .filter(field -> field.numNulls() < rowCount)
          .map(FieldMetrics::doubleMetrics)
          .filter(Objects::nonNull)
          .forEach(field -> {
            minValues.put(field.name(), field.minValue());
            maxValues.put(field.name(), field.maxValue());
          });

      PartitionMetrics metrics = MetacatMetrics.create(
          totalSize, totalSize, rowCount, nullCount, nullCounts.build(), minValues.build(), maxValues.build(), genieId);

      File metricsLocation = MetacatMetrics.metricsPath(dataLocation);
      try {
        Files.write(MetacatMetrics.toJson(metrics), metricsLocation, StandardCharsets.UTF_8);
      } catch (IOException e) {
        // only warn; metrics are optional
        LOG.warn("Failed to write metrics to file: {}", metricsLocation, e);
      }
    }
  }

  private static class FieldMetrics {
    private final int pos;
    private final String name;
    private final DataType type;
    private final DoubleMetrics doubleMetrics;
    private long numNulls = 0;

    private FieldMetrics(int pos, StructField field) {
      this.pos = pos;
      this.name = field.name();
      this.type = field.dataType();
      if (NUMERIC_TYPES.contains(type.simpleString().toLowerCase(Locale.ROOT))) {
        this.doubleMetrics = new DoubleMetrics(name);
      } else {
        this.doubleMetrics = null;
      }
    }

    private void update(InternalRow row) {
      if (row.isNullAt(pos)) {
        numNulls += 1;
      } else if (doubleMetrics != null) {
        doubleMetrics.update((Number) row.get(pos, type));
      }
    }

    String name() {
      return name;
    }

    long numNulls() {
      return numNulls;
    }

    DoubleMetrics doubleMetrics() {
      return doubleMetrics;
    }
  }

  private static class DoubleMetrics {
    private final String name;
    private double minValue = Double.MAX_VALUE;
    private double maxValue = -Double.MAX_VALUE;

    private DoubleMetrics(String name) {
      this.name = name;
    }

    void update(Number num) {
      double value = num.doubleValue();
      if (value != value) {
        // skip NaN
        return;
      }

      minValue = value < minValue ? value : minValue;
      maxValue = value > maxValue ? value : maxValue;
    }

    String name() {
      return name;
    }

    double minValue() {
      return minValue;
    }

    double maxValue() {
      return maxValue;
    }
  }
}
