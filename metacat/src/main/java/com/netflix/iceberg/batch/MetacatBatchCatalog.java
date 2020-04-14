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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.netflix.iceberg.metacat.MetacatUtil;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import com.netflix.metacat.shaded.feign.Retryer;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static com.netflix.iceberg.batch.SparkTables.LOCATION;

public class MetacatBatchCatalog implements TableCatalog, SupportsNamespaces {
  private static final String[] DEFAULT_NAMESPACE = new String[] { "default" };

  private Configuration conf = null;
  private String catalogName = null;
  private String metacatHost = null;
  private String jobId = null;
  private String user = null;

  private Client lazyClient = null;
  private SparkSession lazySpark = null;

  private SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }
    return lazySpark;
  }

  @Override
  public String[] defaultNamespace() {
    return DEFAULT_NAMESPACE;
  }

  @Override
  public String[][] listNamespaces() {
    CatalogDto catalog = reusedClient().getApi().getCatalog(catalogName);
    return catalog.getDatabases().stream()
        .map(db -> new String[]{db})
        .toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    switch (namespace.length) {
      case 0:
        return listNamespaces();
      case 1:
        // metacat databases don't support nesting, so they are all empty
        return new String[0][];
      default:
        throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    try {
      DatabaseDto db = reusedClient().getApi().getDatabase(catalogName, namespace[0],
          true /* include user metadata */,
          false /* include table names */);

      return ImmutableMap.copyOf(db.getMetadata());

    } catch (MetacatNotFoundException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    Preconditions.checkArgument(namespace.length == 1, "Invalid database name: %s", Namespace.of(namespace));

    DatabaseCreateRequestDto createRequest = new DatabaseCreateRequestDto();
    ImmutableMap.Builder<String, String> metaBuilder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      if (LOCATION.equals(entry.getKey())) {
        // TODO: update to Metacat that supports this
        throw new UnsupportedOperationException("Metacat doesn't support setting database location");
      } else {
        metaBuilder.put(entry.getKey(), entry.getValue());
      }
    }
    createRequest.setMetadata(metaBuilder.build());

    try {
      reusedClient().getApi().createDatabase(catalogName, namespace[0], createRequest);
    } catch (MetacatAlreadyExistsException e) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    if (namespace.length != 1) {
      throw new NoSuchNamespaceException(namespace);
    }

    Map<String, String> meta = loadNamespaceMetadata(namespace);
    DatabaseCreateRequestDto createRequest = new DatabaseCreateRequestDto();

    // go through the changes and accumulate the new properties in a map builder and the removed ones in a set
    Set<String> removedProperties = Sets.newHashSet();
    ImmutableMap.Builder<String, String> metaBuilder = ImmutableMap.builder();
    for (NamespaceChange change : changes) {
      if (change instanceof NamespaceChange.RemoveProperty) {
        NamespaceChange.RemoveProperty remove = (NamespaceChange.RemoveProperty) change;
        removedProperties.add(remove.property());
      } else if (change instanceof NamespaceChange.SetProperty) {
        NamespaceChange.SetProperty set = (NamespaceChange.SetProperty) change;
        metaBuilder.put(set.property(), set.value());
      }
    }

    // add the existing properties to the builder and suppress any properties in the remove set
    for (Map.Entry<String, String> entry : meta.entrySet()) {
      if (LOCATION.equals(entry.getKey())) {
        // TODO: update to Metacat that supports this
        throw new UnsupportedOperationException("Metacat doesn't support setting database location");
      } else if (!removedProperties.contains(entry.getKey())) {
        metaBuilder.put(entry.getKey(), entry.getValue());
      }
    }

    createRequest.setMetadata(metaBuilder.build());

    try {
      reusedClient().getApi().updateDatabase(catalogName, namespace[0], createRequest);
    } catch (MetacatNotFoundException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace) {
    if (namespace.length != 1) {
      // the namespace does not exist
      return false;
    }

    try {
      reusedClient().getApi().deleteDatabase(catalogName, namespace[0]);
      return true;
    } catch (MetacatNotFoundException e) {
      return false;
    }
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    if (namespace.length != 1) {
      return new Identifier[0];
    }

    try {
      DatabaseDto db = reusedClient().getApi().getDatabase(catalogName, namespace[0],
          false /* include user metadata */,
          true /* include table names */);

      return db.getTables().stream()
          .map(tableName -> Identifier.of(namespace, tableName))
          .toArray(Identifier[]::new);

    } catch (MetacatNotFoundException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public MetacatSparkTable loadTable(Identifier ident) throws NoSuchTableException {
    if (!isValidIdentifier(ident)) {
      throw new NoSuchTableException(ident);
    }

    try {
      String databaseName = ident.namespace()[0];
      String tableName = ident.name();
      TableDto tableDto = reusedClient().getApi().getTable(catalogName, databaseName, tableName,
          true /* include table fields, partition keys */,
          false /* do not include user definition metadata (including ttl settings) */,
          true /* include user data metadata */);
      if (tableDto.getView() != null) {
        throw new NoSuchTableException(ident);
      }
      return new MetacatSparkTable(reusedClient(), catalogName, databaseName, tableName, tableDto);
    } catch (UnsupportedOperationException | MetacatNotFoundException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    Preconditions.checkArgument(isValidIdentifier(ident), "Identifiers must be catalog.database.table: %s", ident);
    String db = ident.namespace()[0];
    String table = ident.name();

    String defaultLocation = MetacatUtil.defaultTableLocation(conf, reusedClient(), catalogName, db, table);
    TableDto newTableInfo = MetacatSparkTable.createTableInfo(
        QualifiedName.ofTable(catalogName, db, table), schema, partitions, properties, defaultLocation);

    try {
      TableDto createdTableInfo = reusedClient().getApi().createTable(catalogName, db, table, newTableInfo);
      return new MetacatSparkTable(reusedClient(), catalogName, db, table, createdTableInfo);

    } catch (MetacatNotFoundException e) {
      throw new NoSuchNamespaceException(ident.namespace());

    } catch (MetacatBadRequestException | MetacatUserMetadataException e) {
      throw new ValidationException(e, "Failed to create table: invalid request: %s", e.getMessage());

    } catch (MetacatAlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    if (!isValidIdentifier(ident)) {
      throw new NoSuchTableException(ident);
    }

    MetacatSparkTable table = loadTable(ident);

    StructType schema = SparkTables.applySchemaChanges(table, changes);

    ImmutableMap.Builder<String, String> newProperties = ImmutableMap.builder();
    Stream.of(changes)
        .filter(TableChange.SetProperty.class::isInstance)
        .map(TableChange.SetProperty.class::cast)
        .filter(set -> !LOCATION.equals(set.property()))
        .forEach(set -> newProperties.put(set.property(), set.value()));

    ImmutableSet.Builder<String> removedProperties = ImmutableSet.builder();
    Stream.of(changes)
        .filter(TableChange.RemoveProperty.class::isInstance)
        .map(TableChange.RemoveProperty.class::cast)
        .map(TableChange.RemoveProperty::property)
        .forEach(removedProperties::add);

    try {
      table.updateTable(schema, newProperties.build(), removedProperties.build());
      return table;

    } catch (MetacatNotFoundException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to) throws NoSuchTableException, TableAlreadyExistsException {
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException(from);
    }

    String fromDatabase = from.namespace()[0];
    switch (to.namespace().length) {
      case 2:
        Preconditions.checkArgument(catalogName.equals(to.namespace()[0]),
            "Cannot move table between catalogs: %s -> %s", catalogName, to.namespace()[0]);
        Preconditions.checkArgument(fromDatabase.equals(to.namespace()[1]),
            "Cannot move table between databases: %s -> %s", fromDatabase, to.namespace()[1]);
        break;
      case 1:
        Preconditions.checkArgument(fromDatabase.equals(to.namespace()[0]),
            "Cannot move table between databases: %s -> %s", fromDatabase, to.namespace()[0]);
        break;
      default:
        throw new IllegalArgumentException("Invalid identifier (too many parts): " + to);
    }

    if (!from.name().equals(to.name())) {
      try {
        reusedClient().getApi().renameTable(catalogName, fromDatabase, from.name(), to.name());
      } catch (MetacatNotFoundException e) {
        throw new NoSuchTableException(from);
      } catch (MetacatAlreadyExistsException e) {
        throw new TableAlreadyExistsException(to);
      }
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    // do not delegate to the Iceberg catalog because this is the same for all tables
    if (!isValidIdentifier(ident)) {
      return false;
    }

    return MetacatUtil.dropTable(reusedClient(), catalogName, ident.namespace()[0], ident.name());
  }

  private boolean isValidIdentifier(Identifier ident) {
    return ident.namespace().length == 1;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    SparkSession spark = lazySparkSession();
    Configuration conf = new Configuration(spark.sparkContext().hadoopConfiguration());
    this.catalogName = name;
    this.metacatHost = MetacatUtil.syncMetacatUri(options, conf);
    this.jobId = conf.get("genie.job.id");
    this.user = MetacatUtil.getUser();
  }

  @Override
  public String name() {
    return catalogName;
  }

  private Client reusedClient() {
    if (lazyClient == null) {
      this.lazyClient = newClient();
    }
    return this.lazyClient;
  }

  private Client newClient() {
    return Client.builder()
        .withClientAppName("spark")
        .withJobId(jobId)
        .withHost(metacatHost)
        .withUserName(user)
        .withDataTypeContext("hive")
        .withRetryer(new Retryer.Default())
        .build();
  }
}
