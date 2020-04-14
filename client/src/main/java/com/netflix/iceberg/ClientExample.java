package com.netflix.iceberg;

import com.netflix.bdp.s3fs.BdpS3FileSystem;
import com.netflix.hadoop.aws.ConfigurableCredentialsProvider;
import com.netflix.iceberg.metacat.MetacatIcebergCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;

public class ClientExample {
  public static void main(String[] argv) {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:/");
    conf.set("fs.s3.impl", BdpS3FileSystem.class.getName());
    conf.set("fs.s3n.impl", BdpS3FileSystem.class.getName());
    conf.set("bdp.s3.credentials-provider", ConfigurableCredentialsProvider.class.getName());
    conf.set("aws.iam.role.arn.default", "your-role-arn");
    conf.set("netflix.metacat.host", "http://metacat.dynprod.netflix.net:7001");
    conf.set("spark.sql.hive.env", "prod");

    Catalog catalog = new MetacatIcebergCatalog(conf, "app-name");
    Namespace namespace = Namespace.of("prodhive", "iceberg");

    for (Record record : IcebergGenerics.read(catalog.loadTable(TableIdentifier.of(namespace, "dual")))
        .reuseContainers()
        .build()) {
      System.err.println(record.toString());
    }
  }
}
