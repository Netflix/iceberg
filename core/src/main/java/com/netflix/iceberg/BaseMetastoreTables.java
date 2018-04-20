/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.exceptions.AlreadyExistsException;
import com.netflix.iceberg.exceptions.NoSuchTableException;
import org.apache.hadoop.conf.Configuration;

public abstract class BaseMetastoreTables implements Tables {
  private final Configuration conf;

  public BaseMetastoreTables(Configuration conf) {
    this.conf = conf;
  }

  public abstract BaseMetastoreTableOperations newTableOps(Configuration conf, String location);

  @Override
  public Table load(String location) {
    TableOperations ops = newTableOps(conf, location);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: " + location);
    }

    return new BaseTable(ops, location);
  }

  public Table create(Schema schema, String location) {
    return create(schema, PartitionSpec.unpartitioned(), location);
  }

  @Override
  public Table create(Schema schema, PartitionSpec spec, String location) {
    TableOperations ops = newTableOps(conf, location);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists: " + location);
    }

    String path = defaultWarehouseLocation(conf, location);
    TableMetadata metadata = TableMetadata.newTableMetadata(ops, schema, spec, path);
    ops.commit(null, metadata);

    return new BaseTable(ops, location);
  }

  abstract String defaultWarehouseLocation(Configuration conf, String location);
}
