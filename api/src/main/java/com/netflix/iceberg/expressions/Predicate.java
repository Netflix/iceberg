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

import com.google.common.base.Joiner;

import java.util.Collection;

/**
 * This is the base class for predicates comparing a field to a literal.
 *
 * @param <R> The type of {@link Reference} to the field, e.g. {@link BoundReference} or {@link NamedReference}
 * @param <L> The type of {@link Literal} being compared to, e.g. {@link ValueLiteral} or {@link CollectionLiteral}
 */
public abstract class Predicate<R extends Reference, L extends Literal> implements Expression {
  private final Operation op;
  private final R ref;
  private final L literal;

  Predicate(Operation op, R ref, L lit) {
    this.op = op;
    this.ref = ref;
    this.literal = lit;
  }

  @Override
  public Operation op() {
    return op;
  }

  public R ref() {
    return ref;
  }

  public L literal() {
    return literal;
  }

  @Override
  public String toString() {
    if (literal() == null) {
      return String.format("%s(%s)", op().toString(), ref());

    } else if (literal() instanceof Collection) {
      return String.format("%s %s (%s)", String.valueOf(ref()), op().toString(), literal().show());

    } else {
      return String.format("%s < %s", String.valueOf(ref()), literal().show());

    }
  }
}
