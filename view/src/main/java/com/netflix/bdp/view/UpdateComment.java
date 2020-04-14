package com.netflix.bdp.view;

import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.CommitFailedException;

/**
 * API for Adding View Column comments
 * <p>
 * When committing, these changes will be applied to the current view metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}.
 */
public interface UpdateComment extends PendingUpdate<Schema> {
    /**
     * Update the comment of a column in the schema.
     * <p>
     * The name is used to find the column to update using {@link Schema#findField(String)}.
     * <p>
     *
     * @param name   name of the column whose comment is being updated
     * @param newDoc replacement comment string for the column
     * @return this for method chaining
     * @throws IllegalArgumentException If name doesn't identify a column in the schema.
     */
    UpdateComment updateColumnDoc(String name, String newDoc);
}
