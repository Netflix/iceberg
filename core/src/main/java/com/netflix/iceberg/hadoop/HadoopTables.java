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

package com.netflix.iceberg.hadoop;

import com.netflix.iceberg.*;
import com.netflix.iceberg.exceptions.AlreadyExistsException;
import com.netflix.iceberg.exceptions.NoSuchTableException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HadoopTables implements Tables, Configurable {
  private Configuration conf;

  public HadoopTables() {
  }

  public HadoopTables(Configuration conf) {
    this.conf = conf;
  }

  public Table load(String location) {
    TableOperations ops = newTableOps(location);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist at location: " + location);
    }

    return new BaseTable(ops, location);
  }

  public Table create(Schema schema, PartitionSpec spec, String location) {
    TableOperations ops = newTableOps(location);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists at location: " + location);
    }

    TableMetadata metadata = TableMetadata.newTableMetadata(ops, schema, spec, location);
    ops.commit(null, metadata);

    return new BaseTable(ops, location);
  }

  private TableOperations newTableOps(String location) {
    return new HadoopTableOperations(new Path(location), conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
