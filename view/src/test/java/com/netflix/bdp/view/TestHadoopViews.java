package com.netflix.bdp.view;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestHadoopViews extends TestHadoopViewBase {
    int readVersionHint() throws IOException {
        return Integer.parseInt(Files.readFirstLine(versionHintFile, Charsets.UTF_8));
    }

    @Test
    public void testView1() throws Exception {
        viewDefinition = new BaseViewDefinition(DEF, SCHEMA, "", new ArrayList<>());
        Map<String, String> properties = new HashMap<>();
        properties.put(CommonViewConstants.ENGINE_VERSION, "test-engine");
        VIEWS.create(tableLocation, viewDefinition, properties);
        Assert.assertTrue("Table location should exist",
                tableDir.exists());
        Assert.assertTrue("Should create metadata folder",
                metadataDir.exists() && metadataDir.isDirectory());
        Assert.assertTrue("Should create v1 metadata",
                version(1).exists() && version(1).isFile());
        Assert.assertFalse("Should not create v2 or newer versions",
                version(2).exists());
        Assert.assertTrue("Should create version hint file",
                versionHintFile.exists());
        Assert.assertEquals("Should write the current version to the hint file",
                1, readVersionHint());
    }

    @Test
    public void testView2() throws Exception {
        VIEWS.replace(tableLocation, viewDefinition, new HashMap<>());
        Assert.assertTrue("Table location should exist",
                tableDir.exists());
        Assert.assertTrue("Should create metadata folder",
                metadataDir.exists() && metadataDir.isDirectory());
        Assert.assertTrue("Should create v1 metadata",
                version(1).exists() && version(1).isFile());
        Assert.assertTrue("Should create v2 metadata",
                version(2).exists() && version(2).isFile());
        Assert.assertFalse("Should not create v3 or newer versions",
                version(3).exists());
        Assert.assertTrue("Should create version hint file",
                versionHintFile.exists());
        Assert.assertEquals("Should write the current version to the hint file",
                2, readVersionHint());
    }

    @Test
    public void testView3() throws Exception {
        VIEWS.replace(tableLocation, viewDefinition, new HashMap<>());
        Assert.assertTrue("Table location should exist",
                tableDir.exists());
        Assert.assertTrue("Should create metadata folder",
                metadataDir.exists() && metadataDir.isDirectory());
        Assert.assertTrue("Should create v1 metadata",
                version(1).exists() && version(1).isFile());
        Assert.assertTrue("Should create v2 metadata",
                version(2).exists() && version(2).isFile());
        Assert.assertTrue("Should create v3 metadata",
                version(3).exists() && version(3).isFile());
        Assert.assertFalse("Should not create v4 or newer versions",
                version(4).exists());
        Assert.assertTrue("Should create version hint file",
                versionHintFile.exists());
        Assert.assertEquals("Should write the current version to the hint file",
                3, readVersionHint());
    }

    @Test
    public void testView4() throws Exception {
        View view = VIEWS.load(tableLocation);
        view.updateProperties().set("version.history.num-entries", "3").commit();
        view.updateProperties().set(ViewProperties.TABLE_COMMENT, "A dummy table comment").commit();
        VIEWS.replace(tableLocation, viewDefinition, new HashMap<>());
        Assert.assertTrue("Table location should exist",
                tableDir.exists());
        Assert.assertTrue("Should create metadata folder",
                metadataDir.exists() && metadataDir.isDirectory());
        Assert.assertTrue("Should create v1 metadata",
                version(1).exists() && version(1).isFile());
        Assert.assertTrue("Should create v2 metadata",
                version(2).exists() && version(2).isFile());
        Assert.assertTrue("Should create v3 metadata",
                version(3).exists() && version(3).isFile());
        Assert.assertTrue("Should create v4 metadata",
                version(4).exists() && version(4).isFile());
        Assert.assertTrue("Should create v5 metadata",
                version(5).exists() && version(5).isFile());
        Assert.assertTrue("Should create v6 metadata",
                version(6).exists() && version(6).isFile());
        Assert.assertFalse("Should not create v7 or newer versions",
                version(7).exists());
        Assert.assertTrue("Should create version hint file",
                versionHintFile.exists());
        Assert.assertEquals("Should write the current version to the hint file",
                6, readVersionHint());
    }

    @Test
    public void testView5() throws Exception {
        VIEWS.drop(tableLocation);
        Assert.assertFalse("Metadata folder should not exist.",
                metadataDir.exists() && metadataDir.isDirectory());
        Assert.assertFalse("Table location should not exist.",
                tableDir.exists());
    }
}
