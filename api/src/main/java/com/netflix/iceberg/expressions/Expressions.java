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

import com.google.common.base.Preconditions;
import com.netflix.iceberg.expressions.Expression.Operation;

import java.util.Collection;

/**
 * Factory methods for creating {@link Expression expressions}.
 */
public class Expressions {
  private Expressions() {
  }

  public static Expression and(Expression left, Expression right) {
    Preconditions.checkNotNull(left, "Left expression cannot be null.");
    Preconditions.checkNotNull(right, "Right expression cannot be null.");
    if (left == alwaysFalse() || right == alwaysFalse()) {
      return alwaysFalse();
    } else if (left == alwaysTrue()) {
      return right;
    } else if (right == alwaysTrue()) {
      return left;
    }
    return new And(left, right);
  }

  public static Expression or(Expression left, Expression right) {
    Preconditions.checkNotNull(left, "Left expression cannot be null.");
    Preconditions.checkNotNull(right, "Right expression cannot be null.");
    if (left == alwaysTrue() || right == alwaysTrue()) {
      return alwaysTrue();
    } else if (left == alwaysFalse()) {
      return right;
    } else if (right == alwaysFalse()) {
      return left;
    }
    return new Or(left, right);
  }

  public static Expression not(Expression child) {
    Preconditions.checkNotNull(child, "Child expression cannot be null.");
    if (child == alwaysTrue()) {
      return alwaysFalse();
    } else if (child == alwaysFalse()) {
      return alwaysTrue();
    } else if (child instanceof Not) {
      return ((Not) child).child();
    }
    return new Not(child);
  }

  public static <T> UnboundUnaryPredicate<T> isNull(String name) {
    return new UnboundUnaryPredicate<>(Operation.IS_NULL, ref(name));
  }

  public static <T> UnboundUnaryPredicate<T> notNull(String name) {
    return new UnboundUnaryPredicate<>(Operation.NOT_NULL, ref(name));
  }

  public static <T> UnboundValuePredicate<T> lessThan(String name, T value) {
    return new UnboundValuePredicate<>(Operation.LT, ref(name), value);
  }

  public static <T> UnboundValuePredicate<T> lessThanOrEqual(String name, T value) {
    return new UnboundValuePredicate<>(Operation.LT_EQ, ref(name), value);
  }

  public static <T> UnboundValuePredicate<T> greaterThan(String name, T value) {
    return new UnboundValuePredicate<>(Operation.GT, ref(name), value);
  }

  public static <T> UnboundValuePredicate<T> greaterThanOrEqual(String name, T value) {
    return new UnboundValuePredicate<>(Operation.GT_EQ, ref(name), value);
  }

  public static <T> UnboundValuePredicate<T> equal(String name, T value) {
    return new UnboundValuePredicate<>(Operation.EQ, ref(name), value);
  }

  public static <T> UnboundValuePredicate<T> notEqual(String name, T value) {
    return new UnboundValuePredicate<>(Operation.NOT_EQ, ref(name), value);
  }

  public static <T> UnboundCollectionPredicate<T> in(String name, Collection<T> values) {
    return new UnboundCollectionPredicate<>(Operation.IN, ref(name), values);
  }

  // TODO: Start using this wherever we have a reference to in
  public static <T> UnboundCollectionPredicate<T> notIn(String name, Collection<T> values) {
    return new UnboundCollectionPredicate<>(Operation.NOT_IN, ref(name), values);
  }

  public static <T> UnboundValuePredicate<T> predicate(Operation op, String name, T value) {
    switch (op) {
      case LT:
        return lessThan(name, value);
      case LT_EQ:
        return lessThanOrEqual(name, value);
      case GT:
        return greaterThan(name, value);
      case GT_EQ:
        return greaterThanOrEqual(name, value);
      case EQ:
        return equal(name, value);
      case NOT_EQ:
        return notEqual(name, value);
      default:
        throw new IllegalArgumentException("Cannot create " + op + " predicate with a collection as a value");
    }
  }

  public static <T> UnboundCollectionPredicate<T> predicate(Operation op, String name, Collection<T> values) {
    switch (op) {
      case IN:
        return in(name, values);

      case NOT_IN:
        return notIn(name, values);

      default:
        throw new IllegalArgumentException("Cannot create " + op + " predicate with a collection as a value");
    }
  }

  public static <T> UnboundUnaryPredicate<T> predicate(Operation op, String name) {
    switch (op) {
      case IS_NULL:
        return isNull(name);

      case NOT_NULL:
        return notNull(name);

      default:
        throw new IllegalArgumentException("Cannot create " + op + " predicate without a value");
    }
  }

  public static True alwaysTrue() {
    return True.INSTANCE;
  }

  public static False alwaysFalse() {
    return False.INSTANCE;
  }

  public static Expression rewriteNot(Expression expr) {
    return ExpressionVisitors.visit(expr, RewriteNot.get());
  }

  static NamedReference ref(String name) {
    return new NamedReference(name);
  }
}
