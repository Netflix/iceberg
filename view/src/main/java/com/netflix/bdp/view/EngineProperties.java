package com.netflix.bdp.view;

public enum EngineProperties {
    /**
     * Engine expects certain properties/attributes to be set on a table/view object. Those are enumerated here.
     */
    // Properties set by Presto follow
    presto_version,
    presto_query_id,
    // Currently no properties are set by Spark
}
