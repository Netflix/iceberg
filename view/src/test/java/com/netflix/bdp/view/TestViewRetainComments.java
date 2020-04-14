package com.netflix.bdp.view;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestViewRetainComments
        extends TestViewBase {

    @Test
    public void testViewRetainComments() {
        // Set some column comments
        Schema schema = new Schema(
                required(3, "c1", Types.IntegerType.get(), "c1 comment"),
                required(4, "c2", Types.StringType.get(), "c2 comment")
        );
        ViewDefinition viewMetadata = ViewDefinition.of("select sum(1) from base_tab", schema,
                "", new ArrayList<>());
        TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

        // Assert that the replaced view has the correct changes
        ViewVersionMetadata viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
        ViewDefinition oldViewMetadata = viewVersionMetadata.definition();
        Assert.assertEquals(oldViewMetadata.schema().toString(), schema.toString());

        // Change the schema, change the name and data type of one of the columns. Column comments are
        // not expected to persist for that column (because of name change). Change the data type of the other
        // column. Column comments are supposed to persist.
        Schema newSchema = new Schema(
                required(1, "c1", Types.StringType.get()),
                required(2, "intData", Types.IntegerType.get())
        );
        viewMetadata = ViewDefinition.of("select c1, intData from base_tab", newSchema,
                 "new catalog", new ArrayList<>());
        TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

        // Assert that the replaced view has the correct changes
        viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
        oldViewMetadata = viewVersionMetadata.definition();
        Assert.assertEquals(oldViewMetadata.schema().columns().get(0).doc(), "c1 comment");
        Assert.assertEquals(oldViewMetadata.schema().columns().get(1).doc(), null);
        Assert.assertEquals(oldViewMetadata.sessionCatalog(), "new catalog");
        Assert.assertEquals(viewVersionMetadata.currentVersion().summary().properties().get(CommonViewConstants.ENGINE_VERSION),
                "TestEngine");

        // Reset by setting some column comments
        schema = new Schema(
                required(3, "c1", Types.IntegerType.get(), "c1 comment"),
                required(4, "intData", Types.StringType.get(), "c2 comment")
        );
        viewMetadata = ViewDefinition.of("select sum(1) from base_tab", schema,
                "", new ArrayList<>());
        TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

        // Change the required/optional attribute and data type of a column, column comment is expected to persist.
        // Null out the column comment by setting the comment to empty string.
        newSchema = new Schema(
                optional(1, "c1", Types.IntegerType.get()),
                required(2, "intData", Types.IntegerType.get(), "")
        );
        viewMetadata = ViewDefinition.of("select c1, intData from base_tab", newSchema,
                "new catalog", new ArrayList<>());
        TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

        // Assert that the replaced view has the correct changes
        viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
        oldViewMetadata = viewVersionMetadata.definition();
        Assert.assertEquals(oldViewMetadata.schema().columns().get(0).doc(), "c1 comment");
        Assert.assertEquals(oldViewMetadata.schema().columns().get(0).isOptional(), true);
        Assert.assertEquals(oldViewMetadata.schema().columns().get(1).doc(), "");
    }
}
