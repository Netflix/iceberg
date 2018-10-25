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

import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TestHelpers;
import com.netflix.iceberg.expressions.Expression.Operation;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;

public class TestExpressionSerialization {
  @Test
  public void testExpressions() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(34, "a", Types.IntegerType.get())
    );

    Expression[] expressions = new Expression[] {
        Expressions.alwaysFalse(),
        Expressions.alwaysTrue(),
        Expressions.lessThan("x", 5),
        Expressions.lessThanOrEqual("y", -3),
        Expressions.greaterThan("z", 0),
        Expressions.greaterThanOrEqual("t", 129),
        Expressions.equal("col", "data"),
        Expressions.notEqual("col", "abc"),
        Expressions.notNull("maybeNull"),
        Expressions.isNull("maybeNull2"),
        Expressions.not(Expressions.greaterThan("a", 10)),
        Expressions.and(Expressions.greaterThanOrEqual("a", 0), Expressions.lessThan("a", 3)),
        Expressions.or(Expressions.lessThan("a", 0), Expressions.greaterThan("a", 10)),
        Expressions.equal("a", 5).bind(schema.asStruct())
    };

    for (Expression expression : expressions) {
      Expression copy = TestHelpers.roundTripSerialize(expression);
      Assert.assertTrue(
          "Expression should equal the deserialized copy: " + expression + " != " + copy,
          equals(expression, copy));
    }
  }

  // You may be wondering why this isn't implemented as Expression.equals. The reason is that
  // expression equality implies equivalence, which is wider than structural equality. For example,
  // lessThan("a", 3) is equivalent to not(greaterThanOrEqual("a", 4)). To avoid confusion, equals
  // only guarantees object identity.

  private static boolean equals(Expression left, Expression right) {
    if (left.op() != right.op()) {
      return false;
    }

    if (left instanceof Predicate) {
      if (!(left.getClass().isInstance(right))) {
        return false;
      }
      return equals((Predicate) left, (Predicate) right);
    }

    switch (left.op()) {
      case FALSE:
      case TRUE:
        return true;
      case NOT:
        return equals(((Not) left).child(), ((Not) right).child());
      case AND:
        return (
            equals(((And) left).left(), ((And) right).left()) &&
            equals(((And) left).right(), ((And) right).right())
        );
      case OR:
        return (
            equals(((Or) left).left(), ((Or) right).left()) &&
            equals(((Or) left).right(), ((Or) right).right())
        );
      default:
        return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static boolean equals(Predicate left, Predicate right) {
    if (left.op() != right.op()) {
      return false;
    }

    if (!equals(left.ref(), right.ref())) {
      return false;
    }

    if (left instanceof UnboundUnaryPredicate) {
      return true;
    }

    if (left instanceof UnboundValuePredicate && right instanceof UnboundValuePredicate) {
      ValueLiteral<?> ll = ((UnboundValuePredicate<?>) left).literal();
      ValueLiteral<?> rl = ((UnboundValuePredicate<?>) right).literal();
      Comparator<Object> comparator = (Comparator<Object>)ll.comparator();
      return comparator.compare(ll.value(), rl.value()) == 0;
    }
    
    if (left instanceof UnboundCollectionPredicate && right instanceof UnboundCollectionPredicate) {
      CollectionLiteral<?> ll = ((UnboundCollectionPredicate<?>) left).literal();
      Object[] l = ll.values().toArray();
      Object[] r = ((UnboundCollectionPredicate<?>) right).literal().values().toArray();
      Comparator<Object> comparator = (Comparator<Object>)ll.comparator();

      if (l.length != r.length) {
        return false;
      }

      return IntStream.range(0, l.length)
        .allMatch(i -> comparator.compare(l[i], r[i]) == 0);
    }

    return false;
  }

  private static boolean equals(Reference left, Reference right) {
    if (left instanceof NamedReference) {
      if (!(right instanceof NamedReference)) {
        return false;
      }

      NamedReference lref = (NamedReference) left;
      NamedReference rref = (NamedReference) right;

      return lref.name.equals(rref.name);

    } else if (left instanceof BoundReference) {
      if (!(right instanceof BoundReference)) {
        return false;
      }

      BoundReference lref = (BoundReference) left;
      BoundReference rref = (BoundReference) right;

      return (
          lref.fieldId() == rref.fieldId() &&
          lref.type().equals(rref.type())
      );
    }

    return false;
  }
}
