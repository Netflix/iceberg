package com.netflix.iceberg.metacat;

import com.google.common.base.Preconditions;
import com.netflix.metacat.client.Client;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Tables;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;

public class PigMetacatTables implements Tables, Configurable {
    private Configuration conf;

    public PigMetacatTables() {
    }

    public PigMetacatTables(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Table create(Schema schema, PartitionSpec spec, String location,
                        Map<String, String> properties, String tableIdentifier) {
        throw new UnsupportedOperationException("Pig does not support table creation.");
    }

    @Override
    public Table load(String tableIdentifier) {
        String[] tableAndView = tableIdentifier.split("[!]", 2);
        String[] s = tableAndView[0].split("[./]", 3);
        ArrayUtils.reverse(s);

        String view = tableAndView.length > 1 ? tableAndView[1] : null;
        String table    = s[0];
        String database = s.length >= 2 ? s[1]: conf.get("netflix.metacat.default.database", "default");
        String catalog  = s.length == 3 ? s[2]: conf.get("netflix.metacat.default.catalog", "prodhive");

        Client client = Client.builder()
                .withClientAppName("Pig")
                .withJobId(conf.get("genie.job.id"))
                .withHost(conf.get("netflix.metacat.host"))
                .withUserName(System.getProperty("user.name"))
                .withDataTypeContext("hive")
                .build();

        TableOperations ops = new MetacatClientOps(conf, client, catalog, database, table);

        if (view != null) {
            List<String> partitionKeys = client
                .getPartitionApi()
                .getPartitionKeys(catalog, database, table, view,
                    null  /* no filter, get all view partitions */,
                    null  /* no sort columns */,
                    null  /* no sort order */,
                    null  /* no offset */,
                    null  /* no limit */);
            return new View(ops, tableAndView[0], view, partitionKeys);
        } else {
            return new BaseTable(ops, tableAndView[0]);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    private static class View extends BaseTable {
        private final String viewName;
        private final Expression filter;

        private View(TableOperations ops, String tableName, String viewName,
                     List<String> partitionKeys) {
            super(ops, tableName);
            this.viewName = viewName;
            this.filter = partitionFilter(ops.current().spec(), partitionKeys);
        }

        @Override
        public TableScan newScan() {
            return super.newScan().filter(filter);
        }

        @Override
        public String toString() {
            return super.toString() + "!" + viewName;
        }
    }

    static Expression partitionFilter(PartitionSpec spec, List<String> partitionKeys) {
        List<PartitionField> fields = spec.fields();
        Types.StructType partitionType = spec.partitionType().asStructType();

        Expression expr = Expressions.alwaysFalse();
        for (String partitionKey : partitionKeys) {
            expr = Expressions.or(expr, partitionFilter(fields, partitionType, partitionKey));
        }

        return expr;
    }

    static Expression partitionFilter(List<PartitionField> fields,
                                      Types.StructType partitionType,
                                      String partitionKey) {
        String[] partitions = partitionKey.split("[/]", 0);
        Expression partitionExpr = Expressions.alwaysTrue();
        for (int i = 0; i < partitions.length; i += 1) {
            String part = partitions[i];
            String[] nameAndValue = part.split("=", 2);
            PartitionField field = fields.get(i);

            Preconditions.checkNotNull(field,
                "Cannot filter using unknown partition: %s", part);
            Preconditions.checkArgument(
                nameAndValue.length == 2 &&
                nameAndValue[0] != null &&
                field.name().equals(nameAndValue[0]),
                "Invalid partition: " + partitions[i]);

            Object value = Conversions.fromPartitionString(
                partitionType.fieldType(field.name()), nameAndValue[1]);
            partitionExpr = and(partitionExpr, equal(field.name(), value));
        }

        return partitionExpr;
    }
}
