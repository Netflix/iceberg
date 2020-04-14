package com.netflix.iceberg.spark.source;

import com.google.common.base.Preconditions;
import java.util.List;
import com.netflix.iceberg.metacat.MetacatSparkCatalog;
import org.apache.iceberg.spark.source.IcebergSource;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;

/**
 * This source is used to construct Iceberg tables when Iceberg is specified directly in the DataFrameReader or
 * DataFrameWriter API. For example:
 * <p>
 * spark.read.format("iceberg").load("db.table")
 * df.write.format("iceberg").save("db.table")
 * <p>
 * The behavior in 2.3.2 is to respect the "catalog" property in options and to parse the path as an identifier. The
 * "catalog" property was not announced, so it is unlikely that it is used. Effectively, this means that Spark is always
 * going through the TableCatalog API for DataFrameReader and DataFrameWriter and is always using the session catalog.
 * Also, V2 was only used if the path doesn't contain "/".
 * <p>
 * 2.4.4 will not have custom behavior in DataFrameReader and DataFrameWriter like previous versions, so this class is
 * used to mimic the behavior of 2.3.2 by parsing path as an identifier. For multi-catalog support this should check
 * whether the first part of a multi-part identifier is a known catalog and use that catalog. Otherwise, this should
 * look up the table in the current catalog and add the current namespace to the identifier for bare table names.
 * If the path contains "/", this should load a Hadoop table.
 */
public class IcebergMetacatSource extends IcebergSource {
  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public SparkTable getTable(CaseInsensitiveStringMap options, StructType readSchema) {
    Preconditions.checkArgument(options.containsKey("path"), "Cannot open table: path is not set");
    String path = options.get("path");

    if (path.contains("/")) {
      // use HadoopTables to load paths
      super.getTable(options, readSchema);
    }

    SparkSession spark = SparkSession.active();
    List<String> nameParts = parse(spark, path);

    if (nameParts.size() == 1) {
      // the identifier has one part, so use the current catalog and namespace if the catalog is an Iceberg catalog
      Identifier ident = Identifier.of(spark.sessionState().catalogManager().currentNamespace(), nameParts.get(0));
      return loadFromCurrent(spark, ident, readSchema);

    } else {
      // the identifier has more than one part. check whether it is a catalog
      String maybeCatalogName = nameParts.get(0);
      MetacatSparkCatalog catalog = findCatalog(spark, maybeCatalogName);
      if (catalog != null) {
        // the first part was a catalog, load the table from the rest of the identifier
        return load(catalog, ident(nameParts.subList(1, nameParts.size())), readSchema);
      }

      // the first part was not a catalog, use the current catalog to load all parts of the identifier
      return loadFromCurrent(spark, ident(nameParts), readSchema);
    }
  }

  private static List<String> parse(SparkSession spark, String identifier) {
    try {
      return JavaConverters.seqAsJavaListConverter(
          spark.sessionState().sqlParser().parseMultipartIdentifier(identifier)).asJava();
    } catch (ParseException e) {
      throw new org.apache.iceberg.exceptions.NoSuchTableException("Cannot parse identifier: " + identifier);
    }
  }

  private static Identifier ident(List<String> nameParts) {
    int lastIndex = nameParts.size() - 1;
    return Identifier.of(nameParts.subList(0, lastIndex).toArray(new String[0]), nameParts.get(lastIndex));
  }

  private static SparkTable loadFromCurrent(SparkSession spark, Identifier ident, StructType readSchema) {
    CatalogPlugin current = spark.sessionState().catalogManager().currentCatalog();
    if (current instanceof MetacatSparkCatalog) {
      return load((MetacatSparkCatalog) current, ident, readSchema);
    }

    throw new IllegalArgumentException(
        String.format("Cannot load table %s with non-Iceberg current catalog: %s", ident, current));
  }

  private static SparkTable load(MetacatSparkCatalog catalog, Identifier ident, StructType readSchema) {
    try {
      Table table = catalog.loadTable(ident);
      if (table instanceof SparkTable) {
        return ((SparkTable) table).withRequestedSchema(readSchema);
      }

      throw new IllegalArgumentException(String.format(
          "Cannot load with schema on a non-Iceberg table: %s", ident));
    } catch (NoSuchTableException e) {
      throw new org.apache.iceberg.exceptions.NoSuchTableException("Cannot find table: %s", ident);
    }
  }

  private static MetacatSparkCatalog findCatalog(SparkSession session, String... catalogNames) {
    for (String catalog : catalogNames) {
      try {
        CatalogPlugin plugin = session.sessionState().catalogManager().catalog(catalog);
        if (plugin instanceof MetacatSparkCatalog) {
          return (MetacatSparkCatalog) plugin;
        }
      } catch (Exception e) {
        // try the next name
      }
    }
    return null;
  }
}
