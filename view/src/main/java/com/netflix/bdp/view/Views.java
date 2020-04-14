package com.netflix.bdp.view;

import java.util.Map;

/**
 * Generic interface for creating and loading a view implementation.
 * <p>
 * The 'viewIdentifier' field should be interpreted by the underlying
 * implementation (e.g. catalog.database.view_name)
 */
public interface Views {

    /**
     * Create a view without replacing any existing view.
     * @param viewIdentifier view name or location
     * @param viewDefinition SQL metadata of the view
     * @param properties Version property genie-id of the operation,
     *                   as well as table properties such as owner, table type,
     *                   common view flag etc.
     */
    void create(String viewIdentifier, ViewDefinition viewDefinition, Map<String, String> properties);

    /**
     * Replaces a view.
     *
     * @param viewIdentifier view name or location
     * @param viewDefinition SQL metadata of the view
     * @param properties     Version property genie-id of the operation,
     *                       as well as table properties such as owner, table type,
     *                       common view flag etc.
     */
    void replace(String viewIdentifier, ViewDefinition viewDefinition, Map<String, String> properties);

    /**
     * Loads a view by name.
     * @param viewIdentifier view name or location
     * @return All the metadata of the view
     */
    View load(String viewIdentifier);

    /**
     * Loads a view by name.
     * @param viewIdentifier view name or location
     * @return SQL metadata of the view
     */
    ViewDefinition loadDefinition(String viewIdentifier);

    /**
     * Drops a view.
     * @param viewIdentifier view name or location
     */
    void drop(String viewIdentifier);

    /**
     * Renames a view.
     * @param oldIdentifier the view identifier of the existing view to rename
     * @param newIdentifier the new view identifier of the view
     */
    default void rename(String oldIdentifier, String newIdentifier) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
