package com.netflix.bdp.view;

import org.apache.iceberg.Schema;

import java.util.List;
import java.util.Objects;

/**
 * SQL metadata for a view
 */
class BaseViewDefinition implements ViewDefinition {
    private final String sql;
    private final Schema schema;
    private final String sessionCatalog;
    private final List<String> sessionNamespace;

    public BaseViewDefinition(String sql, Schema schema,
                              String sessionCatalog, List<String> sessionNamespace) {
        this.sql = sql;
        this.schema = schema;
        this.sessionCatalog = sessionCatalog;
        this.sessionNamespace = sessionNamespace;
    }

    @Override
    public String sql() {
        return sql;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public String sessionCatalog() {
        return sessionCatalog;
    }

    @Override
    public List<String> sessionNamespace() {
        return sessionNamespace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseViewDefinition that = (BaseViewDefinition) o;
        return Objects.equals(sql, that.sql) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(sessionCatalog, that.sessionCatalog) &&
                Objects.equals(sessionNamespace, that.sessionNamespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, schema, sessionCatalog, sessionNamespace);
    }

    @Override
    public String toString() {
        return "BaseViewDefinition{" +
                "sql='" + sql + '\'' +
                ", schema=" + schema +
                ", sessionCatalog='" + sessionCatalog + '\'' +
                ", sessionNamespace=" + sessionNamespace +
                '}';
    }
}