/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.iceberg.pig;

import com.netflix.iceberg.Schema;
import com.netflix.iceberg.types.Types.ListType;
import com.netflix.iceberg.types.Types.LongType;
import com.netflix.iceberg.types.Types.MapType;
import com.netflix.iceberg.types.Types.StringType;
import com.netflix.iceberg.types.Types.StructType;
import org.apache.pig.ResourceSchema;
import org.junit.Test;

import java.io.IOException;

import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.*;

public class SchemaUtilTest {

  @Test
  public void testTupleInMap() throws IOException {
    Schema icebergSchema = new Schema(
        optional(
            1, "nested_list",
            MapType.ofOptional(
                2, 3,
                StringType.get(),
                ListType.ofOptional(
                    4, StructType.of(
                        required(5, "id", LongType.get()),
                        optional(6, "data", StringType.get()))))));

    ResourceSchema pigSchema = SchemaUtil.convert(icebergSchema);
    assertEquals("nested_list:[{element:(id:long,data:chararray)}]", pigSchema.toString()); // The output should contain a nested struct within a list within a map, I think.
  }
}