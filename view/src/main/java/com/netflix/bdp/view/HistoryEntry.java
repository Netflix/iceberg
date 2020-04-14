package com.netflix.bdp.view;

/**
 * View history entry.
 * <p>
 * An entry contains a change to the view state. At the given timestamp, the current version was
 * set to the given version ID.
 */
public interface HistoryEntry {
    /**
     * @return the timestamp in milliseconds of the change
     */
    long timestampMillis();

    /**
     * @return ID of the new current version
     */
    int versionId();
}
