package com.netflix.bdp.view;

import com.google.common.collect.Iterators;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestViewBase {
    // Schema passed to create tables
    public static final Schema SCHEMA = new Schema(
            required(3, "c1", Types.IntegerType.get()),
            required(4, "c2", Types.StringType.get())
    );

    public static final String SQL = "select c1, c2 from base_tab";

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    File tableDir = null;
    File metadataDir = null;

    @Before
    public void setupTable() throws Exception {
        this.tableDir = temp.newFolder();
        this.metadataDir = new File(tableDir, "metadata");
        create(SCHEMA);
    }

    @After
    public void cleanupTables() {
        TestViews.drop("test", metadataDir);
        TestViews.clearTables();
    }

    private BaseView create(Schema schema) {
        ViewDefinition viewMetadata = ViewDefinition.of(SQL, SCHEMA, "", new ArrayList<>());
        TestViews.create(tableDir, "test", viewMetadata, new HashMap<>());
        return TestViews.load(tableDir, "test");
    }

    BaseView load() {
        return TestViews.load(tableDir, "test");
    }

    Integer version() {
        return TestViews.metadataVersion("test");
    }


    static Iterator<Long> ids(Long... ids) {
        return Iterators.forArray(ids);
    }

    static Iterator<DataFile> files(DataFile... files) {
        return Iterators.forArray(files);
    }

    static Iterator<DataFile> files(ManifestFile manifest) {
        return ManifestReader.read(localInput(manifest.path())).iterator();
    }
}
