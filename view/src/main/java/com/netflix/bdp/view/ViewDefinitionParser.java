package com.netflix.bdp.view;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.iceberg.SchemaParser;

class ViewDefinitionParser {
    static ViewDefinition fromJson(JsonNode node) {
        JsonNode schema = node.get("schema");
        JsonNode sessionCatalog = node.get("sessionCatalog");
        JsonNode sessionNamespace = node.get("sessionNamespace");
        return ViewDefinition.of(
                node.get("sql").asText(),
                schema == null ? ViewDefinition.EMPTY_SCHEMA : SchemaParser.fromJson(schema),
                sessionCatalog == null ? "" : sessionCatalog.asText(),
                sessionNamespace == null ? Collections.emptyList() : readStringList(sessionNamespace));
    }

    static void toJson(ViewDefinition view, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("sql", view.sql());
        generator.writeFieldName("schema");
        SchemaParser.toJson(view.schema(), generator);
        generator.writeStringField("sessionCatalog", view.sessionCatalog());
        generator.writeFieldName("sessionNamespace");
        writeStringList(view.sessionNamespace(), generator);
        generator.writeEndObject();
    }

    private static List<String> readStringList(JsonNode node) {
        return StreamSupport.stream(node.spliterator(), false)
                .map(JsonNode::asText)
                .collect(Collectors.toList());
    }

    private static void writeStringList(List<String> strings, JsonGenerator generator) throws IOException {
        generator.writeStartArray();
        for (String s : strings) {
            generator.writeString(s);
        }
        generator.writeEndArray();
    }
}
