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
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class MetacatUtil {
  private MetacatUtil() {
  }

  public static String getUser() {
    // Match the behavior of Hive's Utils.getUser. If HADOOP_USER_NAME is set, Hive will proxy using the session
    // credentials using doAs, so the effective user is HADOOP_USER_NAME. Otherwise, Hive will use the current
    // credentials to get a username.
    if (System.getenv("HADOOP_USER_NAME") != null) {
      return System.getenv("HADOOP_USER_NAME");
    }

    // Use the current credentials to get a username. This is the call made to determine user in Presto, too.
    try {
      return UserGroupInformation.getCurrentUser().getUserName();
    } catch (IOException e) {
      // use the USER environment variable instead
    }

    // If Hadoop environment credentials aren't available, try USER or the Java user.name system property.
    if (System.getenv("USER") != null) {
      return System.getenv("USER");
    } else {
      return System.getProperty("user.name");
    }
  }

  /**
   * Sync the metacat URI between options and conf.
   *
   * @return the metacat URI that is configured for this catalog
   */
  public static String syncMetacatUri(CaseInsensitiveStringMap options, Configuration conf) {
    String metacatUri = options.get("metacat-uri");
    if (metacatUri != null) {
      conf.set("netflix.metacat.host", metacatUri);
      return metacatUri;
    } else {
      return conf.get("netflix.metacat.host", null);
    }
  }

  public static String defaultTableLocation(Configuration conf, Client client,
                                            String catalog, String database, String tableName) {
    DatabaseDto dbInfo = client.getApi().getDatabase(catalog, database,
        false, /* omit user metadata */
        false /* omit table names */ );

    if (dbInfo.getUri() != null) {
      return dbInfo.getUri() + "/" + tableName;
    }

    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");

    return String.format("%s/%s.db/%s", warehouseLocation, database, tableName);
  }

  public static boolean dropTable(Client client, String catalog, String database, String tableName) {
    try {
      client.getApi().deleteTable(catalog, database, tableName);
      return true;
    } catch (MetacatNotFoundException e) {
      return false;
    }
  }
}
