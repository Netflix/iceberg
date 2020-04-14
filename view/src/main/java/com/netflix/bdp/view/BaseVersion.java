package com.netflix.bdp.view;

import com.google.common.base.MoreObjects;

public class BaseVersion implements Version {
    private final int versionId;
    private final Integer parentId;
    private final long timestampMillis;
    private final VersionSummary summary;
    private final ViewDefinition viewDefinition;

    public BaseVersion(int versionId,
                       Integer parentId,
                       long timestampMillis,
                       VersionSummary summary,
                       ViewDefinition viewDefinition) {
        this.versionId = versionId;
        this.parentId = parentId;
        this.timestampMillis = timestampMillis;
        this.summary = summary;
        this.viewDefinition = viewDefinition;
    }

    @Override
    public int versionId() {
        return versionId;
    }

    @Override
    public Integer parentId() {
        return parentId;
    }

    @Override
    public long timestampMillis() {
        return timestampMillis;
    }

    @Override
    public VersionSummary summary() {
        return summary;
    }

    @Override
    public ViewDefinition viewDefinition() {
        return viewDefinition;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", versionId)
                .add("timestamp_ms", timestampMillis)
                .add("summary", summary)
                .add("view_definition", viewDefinition)
                .toString();
    }
}
