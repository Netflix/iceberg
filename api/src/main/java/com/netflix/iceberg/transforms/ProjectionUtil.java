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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.netflix.iceberg.expressions.Expressions.*;

class ProjectionUtil {
  static <T> UnboundValuePredicate<T> truncateInteger(
      String name, BoundValuePredicate<Integer> pred, Transform<Integer, T> transform) {
    int boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
        // adjust closed and then transform ltEq
        return lessThanOrEqual(name, transform.apply(boundary - 1));
      case LT_EQ:
        return lessThanOrEqual(name, transform.apply(boundary));
      case GT:
        // adjust closed and then transform gtEq
        return greaterThanOrEqual(name, transform.apply(boundary + 1));
      case GT_EQ:
        return greaterThanOrEqual(name, transform.apply(boundary));
      case EQ:
        return equal(name, transform.apply(boundary));
      default:
        return null;
    }
  }

  static <T> UnboundValuePredicate<T> truncateLong(
      String name, BoundValuePredicate<Long> pred, Transform<Long, T> transform) {
    long boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
        // adjust closed and then transform ltEq
        return lessThanOrEqual(name, transform.apply(boundary - 1L));
      case LT_EQ:
        return lessThanOrEqual(name, transform.apply(boundary));
      case GT:
        // adjust closed and then transform gtEq
        return greaterThanOrEqual(name, transform.apply(boundary + 1L));
      case GT_EQ:
        return greaterThanOrEqual(name, transform.apply(boundary));
      case EQ:
        return equal(name, transform.apply(boundary));
      default:
        return null;
    }
  }

  static <T> UnboundValuePredicate<T> truncateDecimal(
      String name, BoundValuePredicate<BigDecimal> pred,
      Transform<BigDecimal, T> transform) {
    BigDecimal boundary = pred.literal().value();
    switch (pred.op()) {
      case LT:
        // adjust closed and then transform ltEq
        BigDecimal minusOne = new BigDecimal(
            boundary.unscaledValue().subtract(BigInteger.ONE),
            boundary.scale());
        return lessThanOrEqual(name, transform.apply(minusOne));
      case LT_EQ:
        return lessThanOrEqual(name, transform.apply(boundary));
      case GT:
        // adjust closed and then transform gtEq
        BigDecimal plusOne = new BigDecimal(
            boundary.unscaledValue().add(BigInteger.ONE),
            boundary.scale());
        return greaterThanOrEqual(name, transform.apply(plusOne));
      case GT_EQ:
        return greaterThanOrEqual(name, transform.apply(boundary));
      case EQ:
        return equal(name, transform.apply(boundary));
      default:
        return null;
    }
  }

  static <S, T> UnboundValuePredicate<T> truncateArray(
      String name, BoundValuePredicate<S> pred, Transform<S, T> transform) {

    S boundary = pred.literal().value();

    switch (pred.op()) {
      case LT:
      case LT_EQ:
        return lessThanOrEqual(name, transform.apply(boundary));
      case GT:
      case GT_EQ:
        return greaterThanOrEqual(name, transform.apply(boundary));
      case EQ:
        return equal(name, transform.apply(boundary));
      default:
        return null;
    }
  }

  static <S, T> UnboundCollectionPredicate<T> truncate(
      String name, BoundCollectionPredicate<S> pred, Transform<S, T> transform) {

    Collection<S> boundary = pred.literal().values();
    Collection<T> transformed = boundary.stream().map(transform::apply).collect(Collectors.toList());

    switch (pred.op()) {
      case IN:
        return in(name, transformed);
      case NOT_IN:
        return notIn(name, transformed);
      default:
        return null;
    }
  }
}
