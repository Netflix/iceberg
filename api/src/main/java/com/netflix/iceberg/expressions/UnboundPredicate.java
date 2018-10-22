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

/**
 * This class represents a predicate that has not been bound to a field yet. It has the name of the field, but doesn't
 * know what the type of the field or if it even exists.
 *
 * @param <T> The type of value we're using for the predicate
 * @param <L> The type of {@link Literal} we're using for the predicate
 */
public abstract class UnboundPredicate<T, L extends Literal<T>> extends Predicate<NamedReference, L> {
  UnboundPredicate(Operation op, NamedReference ref, L lit) {
    super(op, ref, lit);
  }

  public abstract Expression bind(Types.StructType struct);
}
