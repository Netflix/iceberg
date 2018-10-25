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
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import java.time.DateTimeException;
import java.util.UUID;


public class TestStringLiteralConversions {
  @Test
  public void testStringToStringLiteral() {
    ValueLiteral<CharSequence> string = Literals.from("abc");
    Assert.assertSame("Should return same instance", string, string.to(Types.StringType.get()));
  }

  @Test
  public void testStringToDateLiteral() {
    ValueLiteral<CharSequence> dateStr = Literals.from("2017-08-18");
    ValueLiteral<Integer> date = dateStr.to(Types.DateType.get());

    // use Avro's date conversion to validate the result
    Schema avroSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    TimeConversions.DateConversion avroConversion = new TimeConversions.DateConversion();
    int avroValue = avroConversion.toInt(
        new LocalDate(2017, 8, 18),
        avroSchema, avroSchema.getLogicalType());

    Assert.assertEquals("Date should match", avroValue, (int) date.value());
  }

  @Test
  public void testStringToTimeLiteral() {
    // use Avro's time conversion to validate the result
    Schema avroSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    TimeConversions.LossyTimeMicrosConversion avroConversion =
        new TimeConversions.LossyTimeMicrosConversion();

    ValueLiteral<CharSequence> timeStr = Literals.from("14:21:01.919");
    ValueLiteral<Long> time = timeStr.to(Types.TimeType.get());

    long avroValue = avroConversion.toLong(
        new LocalTime(14, 21, 1, 919),
        avroSchema, avroSchema.getLogicalType());

    Assert.assertEquals("Time should match", avroValue, (long) time.value());
  }

  @Test
  public void testStringToTimestampLiteral() {
    // use Avro's timestamp conversion to validate the result
    Schema avroSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    TimeConversions.LossyTimestampMicrosConversion avroConversion =
        new TimeConversions.LossyTimestampMicrosConversion();

    // Timestamp with explicit UTC offset, +00:00
    ValueLiteral<CharSequence> timestampStr = Literals.from("2017-08-18T14:21:01.919+00:00");
    ValueLiteral<Long> timestamp = timestampStr.to(Types.TimestampType.withZone());
    long avroValue = avroConversion.toLong(
        new LocalDateTime(2017, 8, 18, 14, 21, 1, 919).toDateTime(DateTimeZone.UTC),
        avroSchema, avroSchema.getLogicalType());

    Assert.assertEquals("Timestamp should match", avroValue, (long) timestamp.value());

    // Timestamp without an explicit zone should be UTC (equal to the previous converted value)
    timestampStr = Literals.from("2017-08-18T14:21:01.919");
    timestamp = timestampStr.to(Types.TimestampType.withoutZone());

    Assert.assertEquals("Timestamp without zone should match UTC",
        avroValue, (long) timestamp.value());

    // Timestamp with an explicit offset should be adjusted to UTC
    timestampStr = Literals.from("2017-08-18T14:21:01.919-07:00");
    timestamp = timestampStr.to(Types.TimestampType.withZone());
    avroValue = avroConversion.toLong(
        new LocalDateTime(2017, 8, 18, 21, 21, 1, 919).toDateTime(DateTimeZone.UTC),
        avroSchema, avroSchema.getLogicalType());

    Assert.assertEquals("Timestamp without zone should match UTC",
        avroValue, (long) timestamp.value());
  }

  @Test(expected = DateTimeException.class)
  public void testTimestampWithZoneWithoutZoneInLiteral() {
    // Zone must be present in literals when converting to timestamp with zone
    ValueLiteral<CharSequence> timestampStr = Literals.from("2017-08-18T14:21:01.919");
    timestampStr.to(Types.TimestampType.withZone());
  }

  @Test(expected = DateTimeException.class)
  public void testTimestampWithoutZoneWithZoneInLiteral() {
    // Zone must not be present in literals when converting to timestamp without zone
    ValueLiteral<CharSequence> timestampStr = Literals.from("2017-08-18T14:21:01.919+07:00");
    timestampStr.to(Types.TimestampType.withoutZone());
  }

  @Test
  public void testStringToUUIDLiteral() {
    UUID expected = UUID.randomUUID();
    ValueLiteral<CharSequence> uuidStr = Literals.from(expected.toString());
    ValueLiteral<UUID> uuid = uuidStr.to(Types.UUIDType.get());

    Assert.assertEquals("UUID should match", expected, uuid.value());
  }

  @Test
  public void testStringToDecimalLiteral() {
    BigDecimal expected = new BigDecimal("34.560");
    ValueLiteral<CharSequence> decimalStr = Literals.from("34.560");
    ValueLiteral<BigDecimal> decimal = decimalStr.to(Types.DecimalType.of(9, 3));

    Assert.assertEquals("Decimal should have scale 3", 3, decimal.value().scale());
    Assert.assertEquals("Decimal should match", expected, decimal.value());

    Assert.assertNull("Wrong scale in conversion should return null",
        decimalStr.to(Types.DecimalType.of(9, 2)));
    Assert.assertNull("Wrong scale in conversion should return null",
        decimalStr.to(Types.DecimalType.of(9, 4)));
  }
}
