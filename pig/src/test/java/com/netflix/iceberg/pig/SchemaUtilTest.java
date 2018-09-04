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
import com.netflix.iceberg.types.Types.IntegerType;
import com.netflix.iceberg.types.Types.ListType;
import com.netflix.iceberg.types.Types.LongType;
import com.netflix.iceberg.types.Types.MapType;
import com.netflix.iceberg.types.Types.StringType;
import com.netflix.iceberg.types.Types.StructType;
import java.io.IOException;
import org.apache.pig.ResourceSchema;
import org.junit.Test;

import static com.netflix.iceberg.types.Types.BooleanType;
import static com.netflix.iceberg.types.Types.DoubleType;
import static com.netflix.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

public class SchemaUtilTest {
  @Test
  public void mapConversions() throws IOException {
    // consistent behavior for maps conversions. The below test case, correctly does not specify map key types
    convertToPigSchema(
        new Schema(
            required(
                1, "a",
                MapType.ofRequired(
                    2, 3,
                    StringType.get(),
                    ListType.ofRequired(
                        4, StructType.of(
                            required(5, "b", LongType.get()),
                            required(6, "c", StringType.get())))))),
        "a:[{element:(b:long,c:chararray)}]",
        "We do not specify the map key type here");

    // struct<a:map<string,map<string,double>>> -> (a:[[double]])
    // As per https://pig.apache.org/docs/latest/basic.html#map-schema. It seems that
    // we  only need to specify value type as keys are always of type chararray
    convertToPigSchema(
        new Schema(
            StructType.of(
                required(1, "a", MapType.ofRequired(
                    2, 3,
                    StringType.get(),
                    MapType.ofRequired(4, 5, StringType.get(), DoubleType.get())))
            ).fields()),
        "(a:[[double]])",
        "A map key type does not need to be specified");
  }

  @Test
  public void topLevelStruct() throws IOException {
    // struct<a:int> -> (a:int)
    convertToPigSchema(
        new Schema(
            StructType.of(
                required(1, "a", IntegerType.get())
            ).fields()),
        "(a:int)",
        "A struct should be mapped to a Pig tuple"
    );
  }

  @Test
  public void doubleWrappingTuples() throws IOException {
    // struct<a:array<struct<b:string>>> -> (a:{(b:chararray)})
    convertToPigSchema(
        new Schema(
            StructType.of(
                required(1, "a", ListType.ofRequired(2, StructType.of(required(3, "b", StringType.get()))))
            ).fields()),
        "(a:{(b:chararray)})",
        "A tuple inside a bag should not be double wrapped");

    // struct<a:array<boolean>> -> "(a:{(boolean)})
    convertToPigSchema(
        new Schema(StructType.of(required(1, "a", ListType.ofRequired(2, BooleanType.get()))).fields()),
        "(a:{(boolean)})",
        "boolean (or anything non-tuple) element inside a bag should be wrapped inside a tuple"
    );
  }

  private static void convertToPigSchema(Schema icebergSchema, String expectedPigSchema, String assertMessage) throws IOException {
    ResourceSchema pigSchema = SchemaUtil.convert(icebergSchema);
    assertEquals(assertMessage, expectedPigSchema, pigSchema.toString());
  }
}