package com.netflix.bdp.view;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestHadoopViewBase {

  // Schema passed to create tables
  static final Schema SCHEMA = new Schema(
          required(3, "c1", Types.IntegerType.get(), "unique ID"),
          required(4, "c2", Types.IntegerType.get())
  );
  static final String DEF = "select * from base";

  static final HadoopViews VIEWS = new HadoopViews(new Configuration());

  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  static File tableDir = null;
  static String tableLocation = null;
  static File metadataDir = null;
  static File versionHintFile = null;
  static ViewDefinition viewDefinition = null;

  @BeforeClass
  public static void setupTable() throws Exception {
    tableDir = temp.newFolder();
    tableLocation = tableDir.toURI().toString();
    metadataDir = new File(tableDir, "metadata");
    versionHintFile = new File(metadataDir, "version-hint.text");
  }


  File version(int i) {
    return new File(metadataDir, "v" + i + ".json");
  }

  int readVersionHint() throws IOException {
    return Integer.parseInt(Files.readFirstLine(versionHintFile, Charsets.UTF_8));
  }
}
