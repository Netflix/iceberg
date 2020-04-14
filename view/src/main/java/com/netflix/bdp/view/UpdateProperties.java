package com.netflix.bdp.view;

import org.apache.iceberg.PendingUpdate;

import java.util.Map;

/**
 * API for updating table properties.
 * <p>
 * Apply returns the updated table properties as a map for validation.
 * <p>
 * When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will be resolved by applying the pending changes to the new table metadata.
 */
public interface UpdateProperties extends PendingUpdate<Map<String, String>> {

    /**
     * Add a key/value property to the table.
     *
     * @param key   a String key
     * @param value a String value
     * @return this for method chaining
     * @throws NullPointerException If either the key or value is null
     */
    UpdateProperties set(String key, String value);

    /**
     * Remove the given property key from the table.
     *
     * @param key a String key
     * @return this for method chaining
     * @throws NullPointerException If the key is null
     */
    UpdateProperties remove(String key);
}
