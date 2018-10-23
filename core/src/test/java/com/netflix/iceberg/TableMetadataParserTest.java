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
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TableMetadataParserTest {
  @Test
  public void testReadNonCompressedMetadata() {
    Schema schema = new Schema(Lists.newArrayList(Types.NestedField.optional(1, "b", Types.BooleanType.get())));
    final TableMetadata expected = TableMetadata.newTableMetadata(null, schema, PartitionSpec.unpartitioned(), "file://tmp/db/table");
    final TableMetadata read = TableMetadataParser.read(null, Files.localInput(getClass().getClassLoader().getResource("old-metadata.json").getFile()));
    Assert.assertEquals(expected.schema().asStruct(), read.schema().asStruct());
    Assert.assertEquals(expected.location(), read.location());
    Assert.assertEquals(expected.lastColumnId(), read.lastColumnId());
    Assert.assertEquals(expected.properties(), read.properties());
  }
}
