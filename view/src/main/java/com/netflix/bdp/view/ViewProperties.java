package com.netflix.bdp.view;

/**
 * View properties that can be set during CREATE/REPLACE view or using updateProperties API.
 */
public class ViewProperties {
    public static final String COMMIT_NUM_RETRIES = "commit.retry.num-retries";
    public static final int COMMIT_NUM_RETRIES_DEFAULT = 4;

    public static final String COMMIT_MIN_RETRY_WAIT_MS = "commit.retry.min-wait-ms";
    public static final int COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100;

    public static final String COMMIT_MAX_RETRY_WAIT_MS = "commit.retry.max-wait-ms";
    public static final int COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60000; // 1 minute

    public static final String COMMIT_TOTAL_RETRY_TIME_MS = "commit.retry.total-timeout-ms";
    public static final int COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 1800000; // 30 minutes

    public static final String VERSION_HISTORY_SIZE = "version.history.num-entries";
    public static final int VERSION_HISTORY_SIZE_DEFAULT = 10;

    public static final String TABLE_COMMENT = "comment";
}
