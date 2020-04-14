package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTables;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * @deprecated from 0.6.0 use {@link MetacatIcebergCatalog}
 */
@Deprecated
public class MetacatTables extends BaseMetastoreTables {

  private final String catalogName;
  private final MetacatIcebergCatalog metacatCatalog;

  public MetacatTables(Configuration conf, String appName, String catalogName) {
    this(conf, appName, null, catalogName);
  }

  public MetacatTables(Configuration conf, String appName, String appId, String catalogName) {
    this.catalogName = catalogName;
    this.metacatCatalog = new MetacatIcebergCatalog(conf, appId, appName);
  }

  protected String name() {
    return catalogName;
  }

  // Tables methods

  @Override
  public Table load(String tableIdentifier) {
    return metacatCatalog.loadTable(toCatalogTableIdentifier(tableIdentifier));
  }

  @Override
  public Table create(Schema schema, PartitionSpec spec, String location, Map<String, String> properties,
                      String tableIdentifier) {
    return metacatCatalog.createTable(toCatalogTableIdentifier(tableIdentifier), schema, spec, location, properties);
  }

  // BaseMetastoreTables methods

  @Override
  public Table load(String database, String table) {
    return metacatCatalog.loadTable(TableIdentifier.of(catalogName, database, table));
  }

  @Override
  public Table create(Schema schema, String database, String table) {
    return create(schema, PartitionSpec.unpartitioned(), database, table);
  }

  @Override
  public Table create(Schema schema, PartitionSpec spec, String database, String table) {
    return create(schema, spec, ImmutableMap.of(), database, table);
  }

  @Override
  public Table create(Schema schema, PartitionSpec spec, Map<String, String> properties,
                      String database, String table) {
    return create(schema, spec, null, properties, database, table);
  }

  @Override
  public Table create(Schema schema, PartitionSpec spec, String location, Map<String, String> properties,
                      String database, String table) {
    return metacatCatalog.createTable(TableIdentifier.of(catalogName, database, table), schema, spec, properties);
  }

  @Override
  public Transaction beginCreate(Schema schema, PartitionSpec spec, String database, String table) {
    return beginCreate(schema, spec, ImmutableMap.of(), database, table);
  }

  @Override
  public Transaction beginCreate(Schema schema, PartitionSpec spec, Map<String, String> properties,
                                 String database, String table) {
    return beginCreate(schema, spec, null, properties, database, table);
  }

  @Override
  public Transaction beginCreate(Schema schema, PartitionSpec spec, String location, Map<String, String> properties,
                                 String database, String table) {
    return metacatCatalog.newCreateTableTransaction(
        TableIdentifier.of(catalogName, database, table), schema, spec, location, properties);
  }

  @Override
  public Transaction beginReplace(Schema schema, PartitionSpec spec,
                                  String database, String table) {
    return beginReplace(schema, spec, ImmutableMap.of(), database, table);
  }

  @Override
  public Transaction beginReplace(Schema schema, PartitionSpec spec, Map<String, String> properties,
                                  String database, String table) {
    return metacatCatalog.newReplaceTableTransaction(
        TableIdentifier.of(catalogName, database, table), schema, spec, properties, true /* create or replace */ );
  }

  @Override
  public boolean drop(String database, String table) {
    Preconditions.checkArgument(database != null && !"default".equals(database),
        "Cannot drop tables from the default database.");
    return metacatCatalog.dropTable(TableIdentifier.of(Namespace.of(catalogName, database), table), false);
  }

  private TableIdentifier toCatalogTableIdentifier(String tableIdentifier) {
    List<String> namespace = Lists.newArrayList();
    Iterable<String> parts = Splitter.on(".").split(tableIdentifier);

    // add catalogName to the start of the identifier
    String lastPart = catalogName;
    for (String part : parts) {
      namespace.add(lastPart);
      lastPart = part;
    }

    // add the default database name if needed
    if (namespace.size() < 2) {
      namespace.add("default");
    }

    return TableIdentifier.of(Namespace.of(namespace.toArray(new String[namespace.size()])), lastPart);
  }
}
