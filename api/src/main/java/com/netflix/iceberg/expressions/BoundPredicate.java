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

/**
 * This class represents a predicate that has been bound to a known field.
 *
 * @param <T> The type of values in the field
 * @param <L> The type of {@link Literal} used for comparison
 */
public abstract class BoundPredicate<T, L extends Literal<T>> extends Predicate<BoundReference<T>, L> {
  BoundPredicate(Operation op, BoundReference<T> ref, L lit) {
    super(op, ref, lit);
  }
}
