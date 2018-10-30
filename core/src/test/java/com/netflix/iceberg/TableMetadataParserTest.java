/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.iceberg;

import avro.shaded.com.google.common.collect.Lists;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.types.Types.BooleanType;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static avro.shaded.com.google.common.collect.Lists.newArrayList;
import static com.netflix.iceberg.PartitionSpec.unpartitioned;
import static com.netflix.iceberg.TableMetadata.newTableMetadata;
import static com.netflix.iceberg.TableMetadataParser.ICEBERG_COMPRESS_METADATA;
import static com.netflix.iceberg.TableMetadataParser.getFileExtension;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;

public class TableMetadataParserTest {

  private final Schema SCHEMA = new Schema(newArrayList(optional(1, "b", BooleanType.get())));
  private final TableMetadata EXPECTED = newTableMetadata(null, SCHEMA, unpartitioned(), "file://tmp/db/table");
  private final Boolean SHOULD_COMPRESS = parseBoolean(getProperty(ICEBERG_COMPRESS_METADATA, "false"));

  @Test
  public void testCompressionProperty() throws IOException {
    final boolean[] props = {true, false};
    for (boolean prop : props) {
      System.setProperty(ICEBERG_COMPRESS_METADATA, String.valueOf(prop));
      final OutputFile outputFile = Files.localOutput(getFileExtension());
      TableMetadataParser.write(EXPECTED, outputFile);
      Assert.assertEquals(prop, isCompressed(getFileExtension()));
      final TableMetadata read = TableMetadataParser.read(null, Files.localInput(new File(getFileExtension())));
      verifyMetadata(read);
    }
  }

  @After
  public void cleanup() throws IOException {
    final boolean[] props = {true, false};
    for (boolean prop : props) {
      System.setProperty(ICEBERG_COMPRESS_METADATA, String.valueOf(prop));
      java.nio.file.Files.deleteIfExists(Paths.get(getFileExtension()));
    }
    System.setProperty(ICEBERG_COMPRESS_METADATA, String.valueOf(SHOULD_COMPRESS));
  }

  private void verifyMetadata(TableMetadata read) {
    Assert.assertEquals(EXPECTED.schema().asStruct(), read.schema().asStruct());
    Assert.assertEquals(EXPECTED.location(), read.location());
    Assert.assertEquals(EXPECTED.lastColumnId(), read.lastColumnId());
    Assert.assertEquals(EXPECTED.properties(), read.properties());
  }

  private boolean isCompressed(String path) throws IOException {
    try (InputStream ignored = new GzipCompressorInputStream(new FileInputStream(new File(path)))) {
      return true;
    } catch (IOException e) {
      if (e.getMessage().equals("Input is not in the .gz format"))
        return false;
      else
        throw e;
    }
  }
}
