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

import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;

public class TestNumericLiteralConversions {
  @Test
  public void testIntegerToLongConversion() {
    ValueLiteral<Integer> lit = com.netflix.iceberg.expressions.Literals.from(34);
    ValueLiteral<Long> longLit = lit.to(Types.LongType.get());

    Assert.assertEquals("Value should match", 34L, (long) longLit.value());
  }

  @Test
  public void testIntegerToFloatConversion() {
    ValueLiteral<Integer> lit = Literals.from(34);
    ValueLiteral<Float> floatLit = lit.to(Types.FloatType.get());

    Assert.assertEquals("Value should match", 34.0F, floatLit.value(), 0.0000000001D);
  }

  @Test
  public void testIntegerToDoubleConversion() {
    ValueLiteral<Integer> lit = Literals.from(34);
    ValueLiteral<Double> doubleLit = lit.to(Types.DoubleType.get());

    Assert.assertEquals("Value should match", 34.0D, doubleLit.value(), 0.0000000001D);
  }

  @Test
  public void testIntegerToDecimalConversion() {
    ValueLiteral<Integer> lit = Literals.from(34);

    Assert.assertEquals("Value should match",
        new BigDecimal("34"), lit.to(Types.DecimalType.of(9, 0)).value());
    Assert.assertEquals("Value should match",
        new BigDecimal("34.00"), lit.to(Types.DecimalType.of(9, 2)).value());
    Assert.assertEquals("Value should match",
        new BigDecimal("34.0000"), lit.to(Types.DecimalType.of(9, 4)).value());
  }

  @Test
  public void testLongToIntegerConversion() {
    ValueLiteral<Long> lit = Literals.from(34L);
    ValueLiteral<Integer> intLit = lit.to(Types.IntegerType.get());

    Assert.assertEquals("Value should match", 34, (int) intLit.value());

    Assert.assertEquals("Values above Integer.MAX_VALUE should be Literals.aboveMax()",
        com.netflix.iceberg.expressions.Literals.aboveMax(), Literals.from((long) Integer.MAX_VALUE + 1L).to(Types.IntegerType.get()));
    Assert.assertEquals("Values below Integer.MIN_VALUE should be Literals.belowMin()",
        com.netflix.iceberg.expressions.Literals.belowMin(), Literals.from((long) Integer.MIN_VALUE - 1L).to(Types.IntegerType.get()));
  }

  @Test
  public void testLongToFloatConversion() {
    ValueLiteral<Long> lit = Literals.from(34L);
    ValueLiteral<Float> floatLit = lit.to(Types.FloatType.get());

    Assert.assertEquals("Value should match", 34.0F, floatLit.value(), 0.0000000001D);
  }

  @Test
  public void testLongToDoubleConversion() {
    ValueLiteral<Long> lit = Literals.from(34L);
    ValueLiteral<Double> doubleLit = lit.to(Types.DoubleType.get());

    Assert.assertEquals("Value should match", 34.0D, doubleLit.value(), 0.0000000001D);
  }

  @Test
  public void testLongToDecimalConversion() {
    ValueLiteral<Long> lit = Literals.from(34L);

    Assert.assertEquals("Value should match",
        new BigDecimal("34"), lit.to(Types.DecimalType.of(9, 0)).value());
    Assert.assertEquals("Value should match",
        new BigDecimal("34.00"), lit.to(Types.DecimalType.of(9, 2)).value());
    Assert.assertEquals("Value should match",
        new BigDecimal("34.0000"), lit.to(Types.DecimalType.of(9, 4)).value());
  }

  @Test
  public void testFloatToDoubleConversion() {
    ValueLiteral<Float> lit = Literals.from(34.56F);
    ValueLiteral<Double> doubleLit = lit.to(Types.DoubleType.get());

    Assert.assertEquals("Value should match", 34.56D, doubleLit.value(), 0.001D);
  }

  @Test
  public void testFloatToDecimalConversion() {
    ValueLiteral<Float> lit = Literals.from(34.56F);

    Assert.assertEquals("Value should round using HALF_UP",
        new BigDecimal("34.6"), lit.to(Types.DecimalType.of(9, 1)).value());
    Assert.assertEquals("Value should match",
        new BigDecimal("34.56"), lit.to(Types.DecimalType.of(9, 2)).value());
    Assert.assertEquals("Value should match",
        new BigDecimal("34.5600"), lit.to(Types.DecimalType.of(9, 4)).value());
  }

  @Test
  public void testDoubleToFloatConversion() {
    ValueLiteral<Double> lit = Literals.from(34.56D);
    ValueLiteral<Float> doubleLit = lit.to(Types.FloatType.get());

    Assert.assertEquals("Value should match", 34.56F, doubleLit.value(), 0.001D);

    // this adjusts Float.MAX_VALUE using multipliers because most integer adjustments are lost by
    // floating point precision.
    Assert.assertEquals("Values above Float.MAX_VALUE should be Literals.aboveMax()",
        com.netflix.iceberg.expressions.Literals.aboveMax(), Literals.from(2 * ((double) Float.MAX_VALUE)).to(Types.FloatType.get()));
    Assert.assertEquals("Values below Float.MIN_VALUE should be Literals.belowMin()",
        com.netflix.iceberg.expressions.Literals.belowMin(), Literals.from(-2 * ((double) Float.MAX_VALUE)).to(Types.FloatType.get()));
  }

  @Test
  public void testDoubleToDecimalConversion() {
    ValueLiteral<Double> lit = Literals.from(34.56D);

    Assert.assertEquals("Value should round using HALF_UP",
        new BigDecimal("34.6"), lit.to(Types.DecimalType.of(9, 1)).value());
    Assert.assertEquals("Value should match",
        new BigDecimal("34.56"), lit.to(Types.DecimalType.of(9, 2)).value());
    Assert.assertEquals("Value should match",
        new BigDecimal("34.5600"), lit.to(Types.DecimalType.of(9, 4)).value());
  }

  @Test
  public void testDecimalToDecimalConversion() {
    ValueLiteral<BigDecimal> lit = Literals.from(new BigDecimal("34.11"));

    Assert.assertSame("Should return identical object when converting to same scale",
        lit, lit.to(Types.DecimalType.of(9, 2)));
    Assert.assertSame("Should return identical object when converting to same scale",
        lit, lit.to(Types.DecimalType.of(11, 2)));

    Assert.assertNull("Changing decimal scale is not allowed",
        lit.to(Types.DecimalType.of(9, 0)));
    Assert.assertNull("Changing decimal scale is not allowed",
        lit.to(Types.DecimalType.of(9, 1)));
    Assert.assertNull("Changing decimal scale is not allowed",
        lit.to(Types.DecimalType.of(9, 3)));
  }
}
