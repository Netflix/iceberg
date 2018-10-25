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

package com.netflix.iceberg.expressions;

import com.netflix.iceberg.TestHelpers;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import java.util.UUID;

public class TestLiteralSerialization {
  @Test
  public void testLiterals() throws Exception {
    ValueLiteral[] literals = new ValueLiteral[] {
        TestLiterals.from(false),
        TestLiterals.from(34),
        TestLiterals.from(35L),
        TestLiterals.from(36.75F),
        TestLiterals.from(8.75D),
        TestLiterals.from("2017-11-29").to(Types.DateType.get()),
        TestLiterals.from("11:30:07").to(Types.TimeType.get()),
        TestLiterals.from("2017-11-29T11:30:07.123").to(Types.TimestampType.withoutZone()),
        TestLiterals.from("2017-11-29T11:30:07.123+01:00").to(Types.TimestampType.withZone()),
        TestLiterals.from("abc"),
        TestLiterals.from(UUID.randomUUID()),
        TestLiterals.from(new byte[] { 1, 2, 3 }).to(Types.FixedType.ofLength(3)),
        TestLiterals.from(new byte[] { 3, 4, 5, 6 }).to(Types.BinaryType.get()),
        TestLiterals.from(new BigDecimal("122.50")),
    };

    for (ValueLiteral<?> lit : literals) {
      checkValue(lit);
    }
  }

  private <T> void checkValue(ValueLiteral<T> lit) throws Exception {
    ValueLiteral<T> copy = TestHelpers.roundTripSerialize(lit);
    Assert.assertEquals("Literal's comparator should consider values equal",
        0, lit.comparator().compare(lit.value(), copy.value()));
    Assert.assertEquals("Copy's comparator should consider values equal",
        0, copy.comparator().compare(lit.value(), copy.value()));
  }
}
