package com.netflix.bdp.view;

import java.util.Map;

/**
 * A version of the view at a point in time.
 * <p>
 * A version consists of a view metadata file.
 * <p>
 * Versions are created by view operations, like Create and Replace.
 */
public interface Version {
    /**
     * Return this version's ID.
     *
     * @return a long ID
     */
    int versionId();

    /**
     * Return this version's parent ID or null.
     *
     * @return a long ID for this version's parent, or null if it has no parent
     */
    Integer parentId();

    /**
     * Return this version's timestamp.
     * <p>
     * This timestamp is the same as those produced by {@link System#currentTimeMillis()}.
     *
     * @return a long timestamp in milliseconds
     */
    long timestampMillis();

    /**
     * Returns the version summary such as the name and genie-id of the operation that created
     * that version of the view
     *
     * @return a version summary
     */
    VersionSummary summary();

    /**
     * Returns the view sql metadata
     * @return View SQL metadata
     */
    ViewDefinition viewDefinition();
}
