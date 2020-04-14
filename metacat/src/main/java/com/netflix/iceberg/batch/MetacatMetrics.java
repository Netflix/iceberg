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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.netflix.dse.mds.PartitionMetrics;
import com.netflix.dse.mds.metric.Bytes;
import com.netflix.dse.mds.metric.CompressedBytes;
import com.netflix.dse.mds.metric.GenieJobId;
import com.netflix.dse.mds.metric.MaxFieldMetric;
import com.netflix.dse.mds.metric.MinFieldMetric;
import com.netflix.dse.mds.metric.NullCount;
import com.netflix.dse.mds.metric.NullCountFieldMetric;
import com.netflix.dse.mds.metric.NumFiles;
import com.netflix.dse.mds.metric.PartitionMetric;
import com.netflix.dse.mds.metric.RowCount;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MetacatMetrics {
  private static final String NUM_FILES_METRIC_NAME = "com.netflix.dse.mds.metric.NumFiles";
  private static final String COMPRESSED_BYTES_METRIC_NAME = "com.netflix.dse.mds.metric.CompressedBytes";
  private static final String BYTES_METRIC_NAME = "com.netflix.dse.mds.metric.Bytes";
  private static final String ROW_COUNT_METRIC = "com.netflix.dse.mds.metric.RowCount";
  private static final String NULL_COUNT_METRIC = "com.netflix.dse.mds.metric.NullCount";
  private static final String NULL_FIELD_COUNT_METRIC = "com.netflix.dse.mds.metric.NullCountFieldMetric";
  private static final String MIN_FIELD_COUNT_METRIC = "com.netflix.dse.mds.metric.MinFieldMetric";
  private static final String MAX_FIELD_COUNT_METRIC = "com.netflix.dse.mds.metric.MaxFieldMetric";
  private static final String GENIE_ID_METRIC = "com.netflix.dse.mds.metric.GenieJobId";
  private static final Set<String> NO_METRICS = Sets.newHashSet();

  static File metricsPath(File file) {
    return new File(file.getParentFile(), "." + file.getName() + ".metrics");
  }

  static String toJson(PartitionMetrics metrics) throws IOException {
    return MAPPER.get().writeValueAsString(toObjectNode(metrics));
  }

  static PartitionMetrics create(long fileLength, long compressedSize, long rowCount,
                                 long nullCount, Map<String, Long> nullValueCounts,
                                 Map<String, Double> minValues, Map<String, Double> maxValues,
                                 String genieId) {
    Bytes bytesMetric = new Bytes();
    bytesMetric.setValue(fileLength);
    CompressedBytes sizeMetric = new CompressedBytes();
    sizeMetric.setValue(compressedSize);
    NumFiles oneFile = new NumFiles();
    oneFile.setValue(1L);
    RowCount rowCountMetric = new RowCount();
    rowCountMetric.setValue(rowCount);
    NullCount nullCountMetric = new NullCount();
    nullCountMetric.setValue(nullCount);
    NullCountFieldMetric nullFieldCounts = new NullCountFieldMetric();
    nullFieldCounts.setValue(nullValueCounts);
    MinFieldMetric minFieldMetric = new MinFieldMetric();
    minFieldMetric.setValue(minValues);
    MaxFieldMetric maxFieldMetric = new MaxFieldMetric();
    maxFieldMetric.setValue(maxValues);

    PartitionMetrics metrics = new PartitionMetrics(NO_METRICS);
    metrics.getMetrics().put(COMPRESSED_BYTES_METRIC_NAME, sizeMetric);
    metrics.getMetrics().put(NUM_FILES_METRIC_NAME, oneFile);
    metrics.getMetrics().put(BYTES_METRIC_NAME, bytesMetric);
    metrics.getMetrics().put(ROW_COUNT_METRIC, rowCountMetric);
    metrics.getMetrics().put(NULL_COUNT_METRIC, nullCountMetric);
    metrics.getMetrics().put(NULL_FIELD_COUNT_METRIC, nullFieldCounts);
    metrics.getMetrics().put(MIN_FIELD_COUNT_METRIC, minFieldMetric);
    metrics.getMetrics().put(MAX_FIELD_COUNT_METRIC, maxFieldMetric);

    if (genieId != null) {
      GenieJobId jobIdMetric = new GenieJobId();
      jobIdMetric.setValue(genieId);
      metrics.getMetrics().put(GENIE_ID_METRIC, jobIdMetric);
    }

    return metrics;
  }

  private static final ThreadLocal<ObjectMapper> MAPPER =
      new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
          return new ObjectMapper();
        }
      };

  @SuppressWarnings("unchecked")
  private static ObjectNode toObjectNode(PartitionMetrics metrics) {
    if (metrics != null) {
      ObjectNode allMetrics = JsonNodeFactory.instance.objectNode();
      ObjectNode metricsMap = allMetrics.putObject("metrics");

      for (Map.Entry<String, PartitionMetric> entry : metrics.getMetrics().entrySet()) {
        String metricClassName = entry.getKey();
        Object value = entry.getValue().getValue();
        ObjectNode metricNode = metricsMap.putObject(metricClassName);

        if (value instanceof Map) {
          ObjectNode valueNode = metricNode.putObject("value");
          for (Map.Entry<String, Object> fieldEntry :
              ((Map<String, Object>) value).entrySet()) {
            addValue(valueNode, fieldEntry.getKey(), fieldEntry.getValue());
          }
        } else {
          addValue(metricNode, "value", value);
        }
      }

      return allMetrics;
    }

    return null;
  }

  private static void addValue(ObjectNode object, String key, Object value) {
    if (value instanceof Long) {
      object.put(key, (Long) value);
    } else if (value instanceof Integer) {
      object.put(key, (Integer) value);
    } else if (value instanceof Double) {
      object.put(key, (Double) value);
    } else if (value instanceof String) {
      object.put(key, (String) value);
    }
  }
}
