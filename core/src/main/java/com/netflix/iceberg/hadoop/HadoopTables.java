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

import com.netflix.iceberg.BaseTable;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.TableOperations;
import com.netflix.iceberg.Tables;
import com.netflix.iceberg.exceptions.AlreadyExistsException;
import com.netflix.iceberg.exceptions.NoSuchTableException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Implementation of Iceberg tables that uses the Hadoop FileSystem
 * to store metadata and manifests.
 */
public class HadoopTables implements Tables, Configurable {
  private Configuration conf;

  public HadoopTables() {
  }

  public HadoopTables(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Loads the table location from a FileSystem path location.
   *
   * @param location a path URI (e.g. hdfs:///warehouse/my_table/)
   * @return
   */
  @Override
  public Table load(String location) {
    TableOperations ops = newTableOps(location);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist at location: " + location);
    }

    return new BaseTable(ops, location);
  }

  /**
   * Create a table using the FileSystem implementation resolve from
   * location.
   *
   * @param schema
   * @param spec
   * @param location a path URI (e.g. hdfs:///warehouse/my_table)
   * @return
   */
  @Override
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
