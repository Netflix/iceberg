package com.netflix.bdp.view;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestViewDefinitionParser {

    @Test
    public void testBasic() throws IOException {
        checkViewDefinitionSerialization(
                "SELECT 'foo' foo",
                new Schema(optional(1, "foo", Types.StringType.get())),
                "cat",
                Arrays.asList("part1", "part2"));
    }

    @Test
    public void testEmptyCatalogNamespace() throws IOException {
        checkViewDefinitionSerialization(
                "SELECT 1 intcol, 'str' strcol",
                new Schema(optional(1, "intcol", Types.IntegerType.get()),
                        optional(2, "strcol", Types.StringType.get())),
                "",
                Collections.emptyList());
    }

    private void checkViewDefinitionSerialization(
            String sql,
            Schema schema,
            String sessionCatalog,
            List<String> sessionNamespace) throws IOException {
        ViewDefinition expected = ViewDefinition.of(sql, schema, sessionCatalog, sessionNamespace);

        Writer jsonWriter = new StringWriter();
        JsonGenerator generator = new JsonFactory().createGenerator(jsonWriter);
        ViewDefinitionParser.toJson(expected, generator);
        generator.flush();

        ViewDefinition actual = ViewDefinitionParser.fromJson(
                JsonUtil.mapper().readValue(jsonWriter.toString(), JsonNode.class));

        Assert.assertEquals(expected, actual);
    }
}