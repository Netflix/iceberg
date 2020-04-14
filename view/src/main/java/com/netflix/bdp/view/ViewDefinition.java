package com.netflix.bdp.view;

import org.apache.iceberg.Schema;

import java.util.List;

public interface ViewDefinition {
    Schema EMPTY_SCHEMA = new Schema();

    static ViewDefinition of(String sql, Schema schema,
                           String sessionCatalog, List<String> sessionNamespace) {
        return new BaseViewDefinition(sql, schema, sessionCatalog,
                sessionNamespace);
    }

    /**
     * Returns the view query SQL text.
     *
     * @return the view query SQL text
     */
    String sql();

    /**
     * Returns the view query output schema.
     *
     * @return the view query output schema
     */
    Schema schema();

    /**
     * Returns the session catalog when the view is created.
     *
     * @return the session catalog
     */
    String sessionCatalog();

    /**
     * Returns the session namespace when the view is created.
     *
     * @return the session namespace
     */
    List<String> sessionNamespace();
}

