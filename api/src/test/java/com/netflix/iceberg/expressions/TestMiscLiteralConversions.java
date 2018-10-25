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

import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class TestMiscLiteralConversions {
  @Test
  public void testIdentityConversions() {
    List<Pair<ValueLiteral<?>, Type>> pairs = Arrays.asList(
        Pair.of(Literals.from(true), Types.BooleanType.get()),
        Pair.of(Literals.from(34), Types.IntegerType.get()),
        Pair.of(Literals.from(34L), Types.LongType.get()),
        Pair.of(Literals.from(34.11F), Types.FloatType.get()),
        Pair.of(Literals.from(34.55D), Types.DoubleType.get()),
        Pair.of(Literals.from("34.55"), Types.DecimalType.of(9, 2)),
        Pair.of(Literals.from("2017-08-18"), Types.DateType.get()),
        Pair.of(Literals.from("14:21:01.919"), Types.TimeType.get()),
        Pair.of(Literals.from("2017-08-18T14:21:01.919"), Types.TimestampType.withoutZone()),
        Pair.of(Literals.from("abc"), Types.StringType.get()),
        Pair.of(Literals.from(UUID.randomUUID()), Types.UUIDType.get()),
        Pair.of(Literals.from(new byte[] {0, 1, 2}), Types.FixedType.ofLength(3)),
        Pair.of(Literals.from(ByteBuffer.wrap(new byte[] {0, 1, 2})), Types.BinaryType.get())
    );

    for (Pair<ValueLiteral<?>, Type> pair : pairs) {
      ValueLiteral<?> lit = pair.first();
      Type type = pair.second();

      // first, convert the literal to the target type (date/times start as strings)
      ValueLiteral<?> expected = lit.to(type);

      // then check that converting again to the same type results in an identical literal
      Assert.assertSame("Converting twice should produce identical values",
          expected, expected.to(type));
    }
  }

  @Test
  public void testBinaryToFixed() {
    ValueLiteral<ByteBuffer> lit = Literals.from(ByteBuffer.wrap(new byte[] {0, 1, 2}));
    ValueLiteral<ByteBuffer> fixedLit = lit.to(Types.FixedType.ofLength(3));
    Assert.assertNotNull("Should allow conversion to correct fixed length", fixedLit);
    Assert.assertEquals("Conversion should not change value",
        lit.value().duplicate(), fixedLit.value().duplicate());

    Assert.assertNull("Should not allow conversion to different fixed length",
        lit.to(Types.FixedType.ofLength(4)));
    Assert.assertNull("Should not allow conversion to different fixed length",
        lit.to(Types.FixedType.ofLength(2)));
  }

  @Test
  public void testFixedToBinary() {
    ValueLiteral<ByteBuffer> lit = Literals.from(ByteBuffer.wrap(new byte[] {0, 1, 2}));
    ValueLiteral<ByteBuffer> binaryLit = lit.to(Types.BinaryType.get());
    Assert.assertNotNull("Should allow conversion to binary", binaryLit);
    Assert.assertEquals("Conversion should not change value",
        lit.value().duplicate(), binaryLit.value().duplicate());
  }

  @Test
  public void testInvalidBooleanConversions() {
    testInvalidConversions(Literals.from(true),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.DecimalType.of(9, 2),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidIntegerConversions() {
    testInvalidConversions(Literals.from(34),
        Types.BooleanType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidLongConversions() {
    testInvalidConversions(Literals.from(34L),
        Types.BooleanType.get(),
        Types.DateType.get(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidFloatConversions() {
    testInvalidConversions(Literals.from(34.11F),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidDoubleConversions() {
    testInvalidConversions(Literals.from(34.11D),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidDateConversions() {
    testInvalidConversions(Literals.from("2017-08-18").to(Types.DateType.get()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.DecimalType.of(9, 4),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidTimeConversions() {
    testInvalidConversions(
        Literals.from("14:21:01.919").to(Types.TimeType.get()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.DecimalType.of(9, 4),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidTimestampConversions() {
    testInvalidConversions(
        Literals.from("2017-08-18T14:21:01.919").to(Types.TimestampType.withoutZone()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.TimeType.get(),
        Types.DecimalType.of(9, 4),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidDecimalConversions() {
    testInvalidConversions(Literals.from(new BigDecimal("34.11")),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.DecimalType.of(9, 4),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidStringConversions() {
    // Strings can be used for types that are difficult to construct, like decimal or timestamp,
    // but are not intended to support parsing strings to any type
    testInvalidConversions(Literals.from("abc"),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidUUIDConversions() {
    testInvalidConversions(Literals.from(UUID.randomUUID()),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.DecimalType.of(9, 2),
        Types.StringType.get(),
        Types.FixedType.ofLength(1),
        Types.BinaryType.get()
    );
  }

  @Test
  public void testInvalidFixedConversions() {
    testInvalidConversions(Literals.from(ByteBuffer.wrap(new byte[] {0, 1, 2})),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.DecimalType.of(9, 2),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1)
    );
  }

  @Test
  public void testInvalidBinaryConversions() {
    testInvalidConversions(Literals.from(ByteBuffer.wrap(new byte[] {0, 1, 2})),
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.DecimalType.of(9, 2),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(1)
    );
  }

  private void testInvalidConversions(ValueLiteral<?> lit, Type... invalidTypes) {
    for (Type type : invalidTypes) {
      Assert.assertNull(
          lit.value().getClass().getName() + " literal to " + type + " is not allowed",
          lit.to(type));
    }
  }

  private static class Pair<X, Y> {
    public static <X, Y> Pair<X, Y> of(X x, Y y) {
      return new Pair<>(x, y);
    }

    private final X x;
    private final Y y;

    private Pair(X x, Y y) {
      this.x = x;
      this.y = y;
    }

    public X first() {
      return x;
    }

    public Y second() {
      return y;
    }
  }
}
