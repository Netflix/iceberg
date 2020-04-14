package com.netflix.bdp.view;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.JsonUtil;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;

public class VersionParser {

    private static final String VERSION_ID = "version-id";
    private static final String PARENT_VERSION_ID = "parent-version-id";
    private static final String TIMESTAMP_MS = "timestamp-ms";
    private static final String SUMMARY = "summary";
    private static final String OPERATION = "operation";
    private static final String VIEW_DEFINITION = "view-definition";

    static void toJson(Version version, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_ID, version.versionId());
        if (version.parentId() != null) {
            generator.writeNumberField(PARENT_VERSION_ID, version.parentId());
        }
        generator.writeNumberField(TIMESTAMP_MS, version.timestampMillis());

        // Write the summary map
        if (version.summary() != null && version.summary().properties().size() > 0) {
            generator.writeObjectFieldStart(SUMMARY);
            generator.writeStringField(OPERATION, version.summary().properties.get(OPERATION) == null ? "N/A" :
                    version.summary().properties.get(OPERATION));
            generator.writeStringField(CommonViewConstants.GENIE_ID,
                    version.summary().properties.get(CommonViewConstants.GENIE_ID) == null ? "N/A" :
                    version.summary().properties.get(CommonViewConstants.GENIE_ID));
            generator.writeStringField(CommonViewConstants.ENGINE_VERSION,
                    version.summary().properties.get(CommonViewConstants.ENGINE_VERSION) == null ? "N/A" :
                    version.summary().properties.get(CommonViewConstants.ENGINE_VERSION));
            generator.writeEndObject();
        }

        generator.writeFieldName(VIEW_DEFINITION);
        ViewDefinitionParser.toJson(version.viewDefinition(), generator);
        generator.writeEndObject();
    }

    public static String toJson(Version version) {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
            generator.useDefaultPrettyPrinter();
            toJson(version, generator);
            generator.flush();
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to write json for: %s", version);
        }
    }

    static Version fromJson(JsonNode node) {
        Preconditions.checkArgument(node.isObject(),
                "Cannot parse table version from a non-object: %s", node);

        int versionId = JsonUtil.getInt(VERSION_ID, node);
        Integer parentId = null;
        if (node.has(PARENT_VERSION_ID)) {
            parentId = JsonUtil.getInt(PARENT_VERSION_ID, node);
        }
        long timestamp = JsonUtil.getLong(TIMESTAMP_MS, node);

        VersionSummary summary = null;
        String operation = null;
        if (node.has(SUMMARY)) {
            JsonNode sNode = node.get(SUMMARY);
            Preconditions.checkArgument(sNode != null && !sNode.isNull() && sNode.isObject(),
                    "Cannot parse summary from non-object value: %s", sNode);

            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            Iterator<String> fields = sNode.fieldNames();
            while (fields.hasNext()) {
                String field = fields.next();
                if (field.equals(OPERATION)) {
                    builder.put(OPERATION, JsonUtil.getString(field, sNode));
                } else if (field.equals(CommonViewConstants.GENIE_ID)) {
                    builder.put(CommonViewConstants.GENIE_ID, JsonUtil.getString(field, sNode));
                } else if (field.equals(CommonViewConstants.ENGINE_VERSION)) {
                    builder.put(CommonViewConstants.ENGINE_VERSION, JsonUtil.getString(field, sNode));
                } else {
                    builder.put(field, JsonUtil.getString(field, sNode));
                }
            }
            summary = new VersionSummary(builder.build());
        }
        ViewDefinition viewMetadata;
        if (node.has(VIEW_DEFINITION)) {
            JsonNode viewMetadataNode = node.get(VIEW_DEFINITION);
            viewMetadata = ViewDefinitionParser.fromJson(viewMetadataNode);
        } else {
            throw new RuntimeIOException("Failed to read view metadata from json: %s", node);
        }
        return new BaseVersion(versionId, parentId, timestamp, summary, viewMetadata);
    }

    public static Version fromJson(String json) {
        try {
            return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to read version from json: %s", json);
        }
    }
}
