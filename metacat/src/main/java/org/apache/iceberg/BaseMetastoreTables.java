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

package org.apache.iceberg;

import java.util.Map;

/**
 * This class was replaced by {@link BaseMetastoreCatalog}, but its methods are part of the contract for MetacatTables.
 */
public abstract class BaseMetastoreTables implements Tables {

  public abstract Table load(String database, String table);

  public abstract Table create(Schema schema, String database, String table);

  public abstract Table create(Schema schema, PartitionSpec spec, String database, String table);

  public abstract Table create(Schema schema, PartitionSpec spec, Map<String, String> properties,
                               String database, String table);

  public abstract Table create(Schema schema, PartitionSpec spec, String location, Map<String, String> properties,
                               String database, String table);

  public abstract Transaction beginCreate(Schema schema, PartitionSpec spec, String database, String table);

  public abstract Transaction beginCreate(Schema schema, PartitionSpec spec, Map<String, String> properties,
                                          String database, String table);

  public abstract Transaction beginCreate(Schema schema, PartitionSpec spec, String location,
                                          Map<String, String> properties, String database, String table);

  public abstract Transaction beginReplace(Schema schema, PartitionSpec spec, String database, String table);

  public abstract Transaction beginReplace(Schema schema, PartitionSpec spec, Map<String, String> properties,
                                           String database, String table);

  public abstract boolean drop(String database, String table);
}
