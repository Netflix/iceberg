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

package com.netflix.iceberg.transforms;

import com.netflix.iceberg.expressions.*;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static com.netflix.iceberg.expressions.Expression.Operation.IS_NULL;
import static com.netflix.iceberg.expressions.Expression.Operation.NOT_NULL;

enum Dates implements Transform<Integer, Integer> {
  YEAR(ChronoUnit.YEARS, "year"),
  MONTH(ChronoUnit.MONTHS, "month"),
  DAY(ChronoUnit.DAYS, "day");

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private final ChronoUnit granularity;
  private final String name;

  Dates(ChronoUnit granularity, String name) {
    this.granularity = granularity;
    this.name = name;
  }

  @Override
  public Integer apply(Integer days) {
    if (granularity == ChronoUnit.DAYS) {
      return days;
    }
    return (int) granularity.between(EPOCH, EPOCH.plusDays(days));
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.DATE;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.IntegerType.get();
  }

  @Override
  public UnboundPredicate<Integer, ?> project(String name, BoundPredicate<Integer, ?> pred) {
    if (pred instanceof BoundValuePredicate) {
      return ProjectionUtil.truncateInteger(name, (BoundValuePredicate<Integer>)pred, this);
    }

    if (pred instanceof BoundUnaryPredicate) {
      return ((BoundUnaryPredicate<Integer>) pred).unbind(name);
    }

    if (pred instanceof BoundCollectionPredicate) {
      return ProjectionUtil.truncate(name, (BoundCollectionPredicate<Integer>) pred, this);
    }

    throw new UnsupportedOperationException();
  }

  @Override
  public UnboundPredicate<Integer, ?> projectStrict(String name, BoundPredicate<Integer, ?> predicate) {
    return null;
  }

  @Override
  public String toHumanString(Integer value) {
    if (value == null) {
      return "null";
    }

    switch (granularity) {
      case YEARS:
        return TransformUtil.humanYear(value);
      case MONTHS:
        return TransformUtil.humanMonth(value);
      case DAYS:
        return TransformUtil.humanDay(value);
      default:
        throw new UnsupportedOperationException("Unsupported time unit: " + granularity);
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
