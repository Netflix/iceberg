package com.netflix.bdp.view;

import java.util.List;
import java.util.Map;

/**
 * Interface for view definition.
 */
public interface View {

    /**
     * Get the current {@link Version version} for this view, or null if there are no versions.
     *
     * @return the current view version.
     */
    Version currentVersion();

    /**
     * Get the {@link Version versions} of this view.
     *
     * @return an Iterable of versions of this view.
     */
    Iterable<Version> versions();

    /**
     * Get a {@link Version version} in this view by ID.
     *
     * @param versionId version ID
     * @return a Version, or null if the ID cannot be found
     */
    Version version(int versionId);

    /**
     * Get the version history of this table.
     *
     * @return a list of {@link HistoryEntry}
     */
    List<HistoryEntry> history();

    /**
     * Return a map of string properties for this view.
     *
     * @return this view's properties map
     */
    Map<String, String> properties();

    /**
     * Update view properties and commit the changes.
     *
     * @return a new {@link UpdateProperties}
     */
    UpdateProperties updateProperties();
}
