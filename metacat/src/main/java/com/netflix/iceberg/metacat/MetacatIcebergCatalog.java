package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.KSGatewayListener;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.shaded.feign.Retryer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

public class MetacatIcebergCatalog extends BaseMetastoreCatalog {

  private static boolean initialized = false;

  private static void initialize(String appName, String appId, Configuration conf) {
    if (!MetacatIcebergCatalog.initialized) {
      synchronized (MetacatIcebergCatalog.class) {
        if (!MetacatIcebergCatalog.initialized) {
          MetacatIcebergCatalog.initialized = true;
          KSGatewayListener.initialize(appName, appId, conf);
        }
      }
    }
  }

  private Configuration conf;
  private final String metacatHost;
  private final String jobId;
  private final String user;
  private final String appName;
  private final Client dbClient;

  public MetacatIcebergCatalog(Configuration conf, String appName) {
    this(conf, null, appName);
  }

  public MetacatIcebergCatalog(Configuration conf, String appId, String appName) {
    this.conf = conf;
    this.metacatHost = conf.get("netflix.metacat.host");
    this.jobId = conf.get("genie.job.id");
    this.user = MetacatUtil.getUser();
    this.appName = appName;
    this.dbClient = newClient();

    MetacatIcebergCatalog.initialize(appName, appId, conf);
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.hasNamespace() && tableIdentifier.namespace().levels().length == 2;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);
    return new MetacatClientOps(conf, newClient(), catalog, database, tableIdentifier.name());
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);
    return MetacatUtil.defaultTableLocation(conf, dbClient, catalog, database, tableIdentifier.name());
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.putAll(properties);

    if (properties.containsKey("provider") && !properties.containsKey(TableProperties.DEFAULT_FILE_FORMAT)) {
      propertiesBuilder.put(TableProperties.DEFAULT_FILE_FORMAT, properties.get("provider"));
    }

    return super.createTable(identifier, schema, spec, propertiesBuilder.build());
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    if (!isValidIdentifier(tableIdentifier)) {
      return false;
    }

    String catalog = tableIdentifier.namespace().level(0);
    String database = tableIdentifier.namespace().level(1);
    String tableName = tableIdentifier.name();

    return MetacatUtil.dropTable(newClient(), catalog, database, tableName);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException("Identifiers must be catalog.database.table: %s", from);
    }
    Preconditions.checkArgument(isValidIdentifier(to), "Identifiers must be catalog.database.table: %s", to);

    String fromCatalog = from.namespace().level(0);
    String toCatalog = to.namespace().level(0);
    Preconditions.checkArgument(fromCatalog.equals(toCatalog),
        "Cannot move table between catalogs: from=%s and to=%s", fromCatalog, toCatalog);

    String fromDatabase = from.namespace().level(1);
    String toDatabase = to.namespace().level(1);
    Preconditions.checkArgument(fromDatabase.equals(toDatabase),
        "Cannot move table between databases: from=%s and to=%s", fromDatabase, toDatabase);

    String fromTableName = from.name();
    String toTableName = to.name();
    if (!fromTableName.equals(toTableName)) {
      try {
        newClient().getApi().renameTable(fromCatalog, fromDatabase, fromTableName, toTableName);
      } catch (MetacatNotFoundException e) {
        throw new NoSuchTableException(e, "Table does not exist: %s", from);
      } catch (MetacatAlreadyExistsException e) {
        throw new AlreadyExistsException(e, "Table already exists: %s", to);
      }
    }
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return ImmutableList.of();
  }

  @Override
  protected String fullTableName(String catalogName, TableIdentifier identifier) {
    // identifiers for this catalog include the catalog name already
    return identifier.toString();
  }

  @Override
  protected String name() {
    return metacatHost;
  }

  private Client newClient() {
    return Client.builder()
        .withClientAppName(appName)
        .withJobId(jobId)
        .withHost(metacatHost)
        .withUserName(user)
        .withDataTypeContext("hive")
        .withRetryer(new Retryer.Default())
        .build();
  }
}
