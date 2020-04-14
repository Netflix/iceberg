package com.netflix.iceberg.pig;

import org.apache.commons.lang.ArrayUtils;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.TableDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;

/**
 * This is a hack for pig to allow continued use of DseStorage by overwriting the
 * FuncSpec if we detect the table is an iceberg table.
 */
public class IcebergFuncSpecResolver implements BiFunction<Properties, String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergFuncSpecResolver.class);
  private static final String DEFAULT_ICEBERG_STORAGE = "org.apache.iceberg.pig.IcebergStorage";

  @Override
  public String apply(Properties props, String tableIdentifier) {
    String funcClassName = props.getProperty("pig.resolver.funcspec.classname");
    if (funcClassName != null && funcClassName.endsWith("DseStorage")) {
      LOG.info("Found DseStorage LoadFunc: " + funcClassName);
      try {
        LOG.info("Looking up table: " + tableIdentifier);

        String[] tableAndView = tableIdentifier.split("[!]", 2); // remove the view name, if present
        String[] s = tableAndView[0].split("[./]");
        ArrayUtils.reverse(s);

        String table = s[0];
        String database = s.length >= 2 ? s[1] : props.getProperty("netflix.metacat.default.database", "default");
        String catalog = s.length == 3 ? s[2] : props.getProperty("netflix.metacat.default.catalog", "prodhive");

        Client client = Client.builder()
            .withHost(props.getProperty("netflix.metacat.host"))
            .withDataTypeContext("hive")
            .withUserName("pig-iceberg-resolver")
            .withClientAppName("dse-storage")
            .build();

        TableDto tableDto = client.getApi().getTable(catalog, database, table, true, true, false);

        Map<String, String> metadata = tableDto.getMetadata();
        if (metadata.containsKey("table_type") && metadata.get("table_type").equalsIgnoreCase("iceberg")) {
          String icebergStorageImpl = props.getProperty("netflix.pig.iceberg.storage.impl", DEFAULT_ICEBERG_STORAGE);
          LOG.info(tableIdentifier + " is an iceberg table.  Using impl: " + icebergStorageImpl);

          return icebergStorageImpl;
        }

      } catch (Exception e) {
        LOG.error("Failed to resolve table metadata for: " + tableIdentifier, e);
      }
    } else {
      LOG.info("Using original LoadFunc: " + funcClassName);
    }

    return null;
  }
}
