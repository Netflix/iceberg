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

import java.io.Serializable;
import java.util.Comparator;

/**
 * Represents a literal fixed value in an expression predicate
 * @param <T> The Java type of the value represented
 */
public interface Literal<T> extends Serializable {
  /**
   * Return a {@link Comparator} for values.
   * @return a comparator for T objects
   */
  Comparator<T> comparator();
}
