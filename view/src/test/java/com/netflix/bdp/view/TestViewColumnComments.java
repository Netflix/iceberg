package com.netflix.bdp.view;

import org.apache.iceberg.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestViewColumnComments extends TestViewBase {

    @Test
    public void testColumnCommentsUpdate() {
        TestViews.TestViewOperations ops = new TestViews.TestViewOperations("test", metadataDir);
        CommentUpdate commentUpdate = new CommentUpdate(ops);
        commentUpdate.updateColumnDoc("c1", "The column name is c1");
        Schema schema = commentUpdate.apply();
        Assert.assertEquals(schema.findField("c1").doc(), "The column name is c1");

        commentUpdate.updateColumnDoc("c1", "The column name is c1 and type is integer");
        schema = commentUpdate.apply();
        Assert.assertEquals(schema.findField("c1").doc(), "The column name is c1 and type is integer");
        commentUpdate.commit();

        commentUpdate.updateColumnDoc("c2", "The column name is c2 and type is integer");
        schema = commentUpdate.apply();
        Assert.assertEquals(schema.findField("c2").doc(), "The column name is c2 and type is integer");

        commentUpdate.updateColumnDoc("c2", "The column name is c2 and type is string");
        commentUpdate.updateColumnDoc("c2", "");
        schema = commentUpdate.apply();
        Assert.assertEquals(schema.findField("c2").doc(), "");
    }
}
