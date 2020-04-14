package com.netflix.bdp.view;

import java.util.HashMap;
import java.util.Map;

public class VersionSummary {
    Map<String, String> properties = new HashMap<>();

    public VersionSummary(Map<String, String> properties) {
        this.properties.putAll(properties);
    }

    public Map<String, String> properties() {
        return properties;
    }
}
