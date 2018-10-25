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

import com.netflix.iceberg.TestHelpers.Row;
import com.netflix.iceberg.expressions.TestLiterals;
import com.netflix.iceberg.expressions.ValueLiteral;
import com.netflix.iceberg.transforms.Transform;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionPaths {
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()),
      Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone())
  );

  @Test
  public void testPartitionPath() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .hour("ts")
        .bucket("id", 10)
        .build();

    Transform hour = spec.getFieldBySourceId(3).transform();
    Transform bucket = spec.getFieldBySourceId(1).transform();

    ValueLiteral<Long> ts = TestLiterals.from("2017-12-01T10:12:55.038194").to(Types.TimestampType.withoutZone());
    Object tsHour = hour.apply(ts.value());
    Object idBucket = bucket.apply(1);

    Row partition = Row.of(tsHour, idBucket);

    Assert.assertEquals("Should produce expected partition key",
        "ts_hour=2017-12-01-10/id_bucket=" + idBucket, spec.partitionToPath(partition));
  }

  @Test
  public void testEscapedStrings() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("data")
        .truncate("data", 10)
        .build();

    Assert.assertEquals("Should escape / as %2F",
        "data=a%2Fb%2Fc%2Fd/data_trunc=a%2Fb%2Fc%2Fd",
        spec.partitionToPath(Row.of("a/b/c/d", "a/b/c/d")));
  }
}
