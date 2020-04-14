package com.netflix.bdp.view;

public class CommonViewConstants {
    /**
     * Constants that can be used by engines to send additional information for view operations
     * being performed.
     */
    public static final String COMMON_VIEW = "common_view";
    public static final String ENGINE_VERSION = "engine_version";
    public static final String GENIE_ID = "genie_id";
    public static final String OPERATION = "operation";

    /**
     * All the properties except 'common_view' are stored in the View's Version Summary.
     * 'operation' is supplied by the library and hence does not need to appear in the enum below.
     * If you add a new constant that is specific to a version of the view, make sure to add it to the enum below.
     */
    protected enum ViewVersionSummaryConstants
    {
        engine_version,
        genie_id
    }
}
