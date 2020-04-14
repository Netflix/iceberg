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

package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.netflix.metacat.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;

class DefinitionMetadata {
  // configuration table properties
  private static final String DATA_TTL_PROP = "janitor.data-ttl-days";
  private static final String DATA_TTL_COLUMN_PROP = "janitor.data-ttl-column";
  private static final String DATA_TTL_METHOD_PROP = "janitor.data-ttl-method";
  private static final String COMMENT_PROP = "comment";
  private static final String VTTS_TIMESTAMP_SECONDS = "vtts.timestamp-utc-seconds";
  private static final String VTTS_TRIGGER_METHOD = "vtts.trigger-method";

  private static final Set<String> RESERVED_PROPERTIES = Sets.newHashSet(
      DATA_TTL_PROP, DATA_TTL_COLUMN_PROP, DATA_TTL_METHOD_PROP, COMMENT_PROP,
      VTTS_TIMESTAMP_SECONDS, VTTS_TRIGGER_METHOD);

  // "definitionMetadata": {
  //   "lifetime": {
  //     "user": "rblue",
  //     "days": -1 or 120
  //   },
  //   "data_hygiene": {
  // 	   "delete_method": "manually deleted" or "by partition column",
  //     "delete_column": "utc_date",
  //     "hour_column": "utc_hour" <-- not supported
  //   }
  // }
  private static final String LIFETIME = "lifetime";
  private static final String USER = "user";
  private static final String DAYS = "days";
  private static final String DATA_HYGIENE = "data_hygiene";
  private static final String METHOD = "delete_method";
  private static final String COLUMN = "delete_column";
  private static final String DATA_TTL_PREFIX = "janitor.";
  private static final String DATA_TTL_MANUAL = "manually deleted";
  private static final String DATA_TTL_BY_PARTITION_COLUMN = "by partition column";
  private static final Set<String> VALID_TTL_METHODS = Sets.newHashSet(DATA_TTL_MANUAL, DATA_TTL_BY_PARTITION_COLUMN);

  // "definitionMetadata": {
  //   "data_dependency": {
  // 	   "valid_thru_utc_ts": 1584457200, <-- in seconds
  //     "valid_thru_utc_ts_trigger": "manual" or "automatic,
  //     "valid_thru_utc_ts_updated_by": "rblue",
  //     "valid_thru_utc_ts_updated_at": 1584461001, <-- in seconds
  //     "valid_thru_utc_ts_genie_job_id": "genie-id" <-- not supported, no Genie ID here
  //   }
  // }
  private static final String VTTS_PREFIX = "vtts.";
  private static final String DATA_DEPENDENCY = "data_dependency";
  private static final String VTTS_SECONDS = "valid_thru_utc_ts";
  private static final String VTTS_TRIGGER = "valid_thru_utc_ts_trigger";
  private static final String VTTS_UPDATE_USER = "valid_thru_utc_ts_updated_by";
  private static final String VTTS_UPDATED_AT_SECONDS = "valid_thru_utc_ts_updated_at";
  private static final String MANUAL = "manual";
  private static final String AUTOMATIC = "automatic";
  private static final Set<String> VALID_VTTS_TRIGGERS = Sets.newHashSet(MANUAL, AUTOMATIC);

  // "definitionMetadata": {
  //   "table_description": "Table doc string"
  // }
  private static final String DESCRIPTION = "table_description";

  static boolean isReservedProperty(String property) {
    return RESERVED_PROPERTIES.contains(property);
  }

  static Map<String, String> reservedProperties(ObjectNode definitionMetadata) {
    if (definitionMetadata != null) {
      ImmutableMap.Builder<String, String> reservedProperties = ImmutableMap.builder();

      if (definitionMetadata.has(LIFETIME)) {
        JsonNode lifetime = definitionMetadata.get(LIFETIME);
        copyNumber(lifetime, DAYS, reservedProperties, DATA_TTL_PROP);
      }

      if (definitionMetadata.has(DATA_HYGIENE)) {
        JsonNode dataHygiene = definitionMetadata.get(DATA_HYGIENE);
        copyString(dataHygiene, METHOD, reservedProperties, DATA_TTL_METHOD_PROP);
        copyString(dataHygiene, COLUMN, reservedProperties, DATA_TTL_COLUMN_PROP);
      }

      if (definitionMetadata.has(DATA_DEPENDENCY)) {
        JsonNode dataDependency = definitionMetadata.get(DATA_DEPENDENCY);
        copyNumber(dataDependency, VTTS_SECONDS, reservedProperties, VTTS_TIMESTAMP_SECONDS);
        copyString(dataDependency, VTTS_TRIGGER, reservedProperties, VTTS_TRIGGER_METHOD);
      }

      copyString(definitionMetadata, DESCRIPTION, reservedProperties, COMMENT_PROP);

      return reservedProperties.build();
    }

    return ImmutableMap.of();
  }

  static ObjectNode buildDefinitionMetadata(TableMetadata base, TableMetadata current) {
    ObjectNode metadata = JsonNodeFactory.instance.objectNode();
    addDescription(metadata, base, current);
    addVTTSProperties(metadata, base, current);
    addJanitorProperties(metadata, base, current);
    addFlinkWatermarkProperties(metadata, base, current);
    return metadata;
  }

  private static void addDescription(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    Map<String, String> updates = changedProperties(
        base != null ? base.properties() : null, current.properties(), COMMENT_PROP);

    String description = updates.get(COMMENT_PROP);
    if (description != null) {
      metadata.put(DESCRIPTION, description);
    }
  }

  private static void addVTTSProperties(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    Map<String, String> updates = changedProperties(
        base != null ? base.properties() : null, current.properties(), VTTS_PREFIX);

    if (updates.isEmpty()) {
      return;
    }

    ObjectNode dataDependency = null;

    String triggerMethod = updates.get(VTTS_TRIGGER_METHOD);
    if (triggerMethod != null) {
      dataDependency = JsonNodeFactory.instance.objectNode();
      String triggerMethodLower = triggerMethod.toLowerCase(Locale.ROOT);
      Preconditions.checkArgument(VALID_VTTS_TRIGGERS.contains(triggerMethodLower),
          "Invalid value for %s: %s (not in %s)", VTTS_TRIGGER_METHOD, triggerMethod, VALID_VTTS_TRIGGERS);
      dataDependency.put(VTTS_TRIGGER, triggerMethodLower);
    }

    String vttsTimestamp = updates.get(VTTS_TIMESTAMP_SECONDS);
    if (vttsTimestamp != null) {
      String currentTriggerMethod = triggerMethod != null ? triggerMethod :
          base != null ? base.properties().get(VTTS_TRIGGER_METHOD) : MANUAL;
      Preconditions.checkArgument(MANUAL.equals(currentTriggerMethod),
          "Cannot manually set VTTS: trigger method is %s", currentTriggerMethod);

      long timestampSeconds = Long.parseLong(vttsTimestamp);
      Preconditions.checkArgument(timestampSeconds < 10000000000L,
          "Invalid value for %s: %s (not in seconds)", VTTS_TIMESTAMP_SECONDS, vttsTimestamp);

      String previousTimestamp = base != null ? base.properties().get(VTTS_TIMESTAMP_SECONDS) : null;
      long previousTimestampSeconds = previousTimestamp != null ? Long.parseLong(previousTimestamp) : Long.MIN_VALUE;
      Preconditions.checkArgument(timestampSeconds >= previousTimestampSeconds,
          "Invalid value for %s: %s (not later than %s)",
          VTTS_TIMESTAMP_SECONDS, vttsTimestamp, previousTimestampSeconds);

      if (dataDependency == null) {
        dataDependency = JsonNodeFactory.instance.objectNode();
      }

      dataDependency.put(VTTS_SECONDS, String.valueOf(timestampSeconds));
      dataDependency.put(VTTS_UPDATE_USER, MetacatUtil.getUser());
      dataDependency.put(VTTS_UPDATED_AT_SECONDS, System.currentTimeMillis() / 1_000);
    }

    if (dataDependency != null) {
      metadata.put(DATA_DEPENDENCY, dataDependency);
    }
  }

  private static void addJanitorProperties(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    Map<String, String> updates = changedProperties(
        base != null ? base.properties() : null, current.properties(), DATA_TTL_PREFIX);

    if (updates.isEmpty()) {
      return;
    }

    String ttlUpdate = updates.get(DATA_TTL_PROP);
    if (ttlUpdate != null) {
      ObjectNode lifetime = JsonNodeFactory.instance.objectNode();

      try {
        lifetime.put(DAYS, Long.parseLong(ttlUpdate));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(String.format("Invalid value for %s: %s", DATA_TTL_PROP, ttlUpdate));
      }

      lifetime.put(USER, MetacatUtil.getUser());
      metadata.put(LIFETIME, lifetime);
    }

    String ttlMethod = updates.get(DATA_TTL_METHOD_PROP);
    String ttlColumn = updates.get(DATA_TTL_COLUMN_PROP);
    if (ttlMethod != null || ttlColumn != null) {
      ObjectNode dataHygiene = JsonNodeFactory.instance.objectNode();

      if (ttlMethod != null) {
        String ttlMethodLower = ttlMethod.toLowerCase(Locale.ROOT);
        Preconditions.checkArgument(VALID_TTL_METHODS.contains(ttlMethodLower),
            "Invalid value for %s: %s (not in %s)", DATA_TTL_METHOD_PROP, ttlMethod, VALID_TTL_METHODS);
        dataHygiene.put(METHOD, ttlMethodLower);
      }

      if (ttlColumn != null) {
        dataHygiene.put(COLUMN, ttlColumn);
      }

      metadata.put(DATA_HYGIENE, dataHygiene);
    }
  }

  private static final String FLINK_WATERMARK_PREFIX = "flink.watermark.";

  private static void addFlinkWatermarkProperties(ObjectNode metadata, TableMetadata base, TableMetadata current) {
    Map<String, String> updates = changedProperties(
        base != null ? base.properties() : null, current.properties(), FLINK_WATERMARK_PREFIX);

    if (updates.isEmpty()) {
      return;
    }

    ObjectNode watermarks = JsonNodeFactory.instance.objectNode();
    for (Map.Entry<String, String> update : updates.entrySet()) {
      watermarks.put(
          update.getKey().replace(FLINK_WATERMARK_PREFIX, ""),
          Long.parseLong(update.getValue()));
    }

    metadata.put("flink.watermarks", watermarks);
  }

  private static Map<String, String> changedProperties(Map<String, String> base, Map<String, String> current,
                                               String prefix) {
    Map<String, String> result = Maps.newHashMap();
    Set<Map.Entry<String, String>> baseSet = base != null ? base.entrySet() : Collections.emptySet();

    for (Map.Entry<String, String> entry : current.entrySet()) {
      // forward the properties that are not in the base set (changed) and match the prefix
      if (!baseSet.contains(entry) && entry.getKey().startsWith(prefix)) {
        result.put(entry.getKey(), entry.getValue());
      }
    }

    return result;
  }

  private static void copyString(JsonNode node, String jsonProperty,
                                 ImmutableMap.Builder<String, String> builder, String property) {
    if (node != null && node.isObject() && node.has(jsonProperty)) {
      JsonNode value = node.get(jsonProperty);
      if (value != null && !value.isNull() && value.isTextual()) {
        builder.put(property, value.asText());
      }
    }
  }

  private static void copyNumber(JsonNode node, String jsonProperty,
                                 ImmutableMap.Builder<String, String> builder, String property) {
    if (node != null && node.isObject() && node.has(jsonProperty)) {
      JsonNode value = node.get(jsonProperty);
      if (value != null && !value.isNull() && value.isNumber()) {
        builder.put(property, value.asText());
      }
    }
  }
}
