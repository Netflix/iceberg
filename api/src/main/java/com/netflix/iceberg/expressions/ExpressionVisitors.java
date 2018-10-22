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

import java.util.Collection;

/**
 * Utils for traversing {@link Expression expressions}.
 */
public class ExpressionVisitors {
  public abstract static class ExpressionVisitor<R> {
    public R alwaysTrue() {
      return null;
    }

    public R alwaysFalse() {
      return null;
    }

    public R not(R result) {
      return null;
    }

    public R and(R leftResult, R rightResult) {
      return null;
    }

    public R or(R leftResult, R rightResult) {
      return null;
    }

    public <T> R predicate(BoundPredicate<T, ?> pred) {
      return null;
    }

    public <T> R predicate(UnboundPredicate<T, ?> pred) { return null; }
  }

  public abstract static class BoundExpressionVisitor<R> extends ExpressionVisitor<R> {
    public <T> R isNull(BoundReference<T> ref) {
      return null;
    }

    public <T> R notNull(BoundReference<T> ref) {
      return null;
    }

    public <T> R lt(BoundReference<T> ref, ValueLiteral<T> lit) {
      return null;
    }

    public <T> R ltEq(BoundReference<T> ref, ValueLiteral<T> lit) {
      return null;
    }

    public <T> R gt(BoundReference<T> ref, ValueLiteral<T> lit) {
      return null;
    }

    public <T> R gtEq(BoundReference<T> ref, ValueLiteral<T> lit) {
      return null;
    }

    public <T> R eq(BoundReference<T> ref, ValueLiteral<T> lit) {
      return null;
    }

    public <T> R notEq(BoundReference<T> ref, ValueLiteral<T> lit) {
      return null;
    }

    public <T> R in(BoundReference<T> ref, CollectionLiteral<T> lit) {
      return null;
    }

    public <T> R notIn(BoundReference<T> ref, CollectionLiteral<T> lit) {
      return null;
    }

    @Override
    public <T> R predicate(BoundPredicate<T, ?> pred) {
      if (pred instanceof BoundUnaryPredicate) {
        switch (pred.op()) {
          case IS_NULL:
            return isNull(pred.ref());
          case NOT_NULL:
            return notNull(pred.ref());
          default:
            throw new UnsupportedOperationException("Unknown unary operation for predicate: " + pred.op());
        }

      } else if (pred instanceof BoundValuePredicate) {
        //noinspection unchecked
        BoundValuePredicate<T> valuePredicate = (BoundValuePredicate<T>) pred;
       
        switch (pred.op()) {
          case LT:
            return lt(valuePredicate.ref(), valuePredicate.literal());
          case LT_EQ:
            return ltEq(valuePredicate.ref(), valuePredicate.literal());
          case GT:
            return gt(valuePredicate.ref(), valuePredicate.literal());
          case GT_EQ:
            return gtEq(valuePredicate.ref(), valuePredicate.literal());
          case EQ:
            return eq(valuePredicate.ref(), valuePredicate.literal());
          case NOT_EQ:
            return notEq(valuePredicate.ref(), valuePredicate.literal());
          default:
            throw new UnsupportedOperationException(
                "Unknown single value operation for predicate: " + valuePredicate.op());
        }

      } else if (pred instanceof BoundCollectionPredicate) {
        //noinspection unchecked
        BoundCollectionPredicate<Collection<T>> collectionPredicate = (BoundCollectionPredicate<Collection<T>>) pred;
        switch (pred.op()) {
          case IN:
            return in(collectionPredicate.ref(), collectionPredicate.literal());
          case NOT_IN:
            return notIn(collectionPredicate.ref(), collectionPredicate.literal());
          default:
            throw new UnsupportedOperationException(
                "Unknown collection operation for predicate: " + collectionPredicate.op());
        }
      } else {

        throw new UnsupportedOperationException(
            "Unknown predicate type " + pred.getClass().getName() + " for op " + pred.op());
      }
    }

    @Override
    public <T> R predicate(UnboundPredicate<T, ?> pred) {
      throw new UnsupportedOperationException("Not a bound predicate: " + pred);
    }
  }

  /**
   * Traverses the given {@link Expression expression} with a {@link ExpressionVisitor visitor}.
   * <p>
   * The visitor will be called to handle each node in the expression tree in postfix order. Result
   * values produced by child nodes are passed when parent nodes are handled.
   *
   * @param expr an expression to traverse
   * @param visitor a visitor that will be called to handle each node in the expression tree
   * @param <R> the return type produced by the expression visitor
   * @return the value returned by the visitor for the root expression node
   */
  @SuppressWarnings("unchecked")
  public static <R> R visit(Expression expr, ExpressionVisitor<R> visitor) {
    if (expr instanceof Predicate) {
      if (expr instanceof BoundPredicate) {
        return visitor.predicate((BoundPredicate<?, ?>) expr);
      } else {
        return visitor.predicate((UnboundPredicate<?, ?>) expr);
      }
    } else {
      switch (expr.op()) {
        case TRUE:
          return visitor.alwaysTrue();
        case FALSE:
          return visitor.alwaysFalse();
        case NOT:
          Not not = (Not) expr;
          return visitor.not(visit(not.child(), visitor));
        case AND:
          And and = (And) expr;
          return visitor.and(visit(and.left(), visitor), visit(and.right(), visitor));
        case OR:
          Or or = (Or) expr;
          return visitor.or(visit(or.left(), visitor), visit(or.right(), visitor));
        default:
          throw new UnsupportedOperationException(
              "Unknown operation: " + expr.op());
      }
    }
  }
}
