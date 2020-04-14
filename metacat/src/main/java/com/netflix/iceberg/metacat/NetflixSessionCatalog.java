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

import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class NetflixSessionCatalog extends SparkSessionCatalog {
  @Override
  protected TableCatalog buildSparkCatalog(String name, CaseInsensitiveStringMap options) {
    SparkSession spark = SparkSession.active();

    String hadoopHiveEnv = spark.sparkContext().hadoopConfiguration().get("spark.sql.hive.env", "prod");
    String hiveEnv = spark.sparkContext().conf().get("spark.sql.hive.env", hadoopHiveEnv);
    String defaultCatalog = hiveEnv + "hive";
    String catalogName = options.getOrDefault("catalog", defaultCatalog);

    CatalogPlugin catalog = spark.sessionState().catalogManager().catalog(catalogName);
    if (catalog instanceof TableCatalog) {
      return (TableCatalog) catalog;
    }

    throw new IllegalArgumentException("Cannot delegate session catalog to non-TableCatalog: " + catalog);
  }

  @Override
  public String toString() {
    return "NetflixSessionCatalog(" + name() + ")";
  }
}
