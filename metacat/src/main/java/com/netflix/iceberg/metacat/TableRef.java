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
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TableRef {
  private static final Logger LOG = LoggerFactory.getLogger(TableRef.class);

  private static final Pattern TABLE_PATTERN = Pattern.compile(
      "(?<table>[^$@]+?)" +
          "(?:(?:@|__)(?<ver1>\\d+))?" +
          "(?:(?:\\$|__)(?<type>" +
          "(?:history|snapshots|manifests|partitions|files|entries|all_data_files|all_manifests|all_entries))" +
          "(?:(?:@|__)(?<ver2>\\d+))?)?");

  static TableRef parse(String rawName) {
    Matcher match = TABLE_PATTERN.matcher(rawName);
    if (match.matches()) {
      try {
        String table = match.group("table");
        String typeStr = match.group("type");
        String ver1 = match.group("ver1");
        String ver2 = match.group("ver2");

        TableType type;
        if (typeStr != null) {
          type = TableType.valueOf(typeStr.toUpperCase(Locale.ROOT));
        } else {
          type = TableType.DATA;
        }

        Long version;
        if (type == TableType.DATA ||
            type == TableType.PARTITIONS ||
            type == TableType.MANIFESTS ||
            type == TableType.FILES ||
            type == TableType.ENTRIES) {
          Preconditions.checkArgument(ver1 == null || ver2 == null,
              "Cannot specify two versions");
          if (ver1 != null) {
            version = Long.parseLong(ver1);
          } else if (ver2 != null) {
            version = Long.parseLong(ver2);
          } else {
            version = null;
          }
        } else {
          Preconditions.checkArgument(ver1 == null && ver2 == null,
              "Cannot use version with table type %s: %s", typeStr, rawName);
          version = null;
        }

        return new TableRef(table, type, version);

      } catch (IllegalArgumentException e) {
        LOG.warn("Failed to parse table name, using {}: {}", rawName, e.getMessage());
      }
    }

    return new TableRef(rawName, TableType.DATA, null);
  }

  private final String table;
  private final TableType type;
  private final Long at;

  private TableRef(String table, TableType type, Long at) {
    this.table = table;
    this.type = type;
    this.at = at;
  }

  public String table() {
    return table;
  }

  public TableType type() {
    return type;
  }

  public Long at() {
    return at;
  }
}
