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

import java.util.stream.Collectors;

/**
 * This class represents a predicate that has been bound to a known field and will be comparing it to a collection of
 * that field's type.
 *
 * @param <T> The type of values in the field
 */
public class BoundCollectionPredicate<T> extends BoundPredicate<T, CollectionLiteral<T>> {
  BoundCollectionPredicate(Operation op, BoundReference<T> ref, CollectionLiteral<T> lit) {
    super(op, ref, lit);
  }

  @Override
  public CollectionLiteral<T> literal() {
    return super.literal();
  }

  @Override
  public Expression negate() {
    return new BoundCollectionPredicate<>(op().negate(), ref(), literal());
  }

  @Override
  public String toString() {
    switch (op()) {
      case IN:
        return String.valueOf(ref()) + " in " +
          literal().values().stream().map(v -> v.toString()).collect(Collectors.joining("(", ", ", ")"));
      case NOT_IN:
        return String.valueOf(ref()) + " not in " +
          literal().values().stream().map(v -> v.toString()).collect(Collectors.joining("(", ", ", ")"));
    }

    return super.toString();
  }
  
  public UnboundCollectionPredicate<T> unbind(String newName) {
    return new UnboundCollectionPredicate<>(op(), new NamedReference(newName), literal());
  }
}
