package com.netflix.iceberg.expressions;

import com.netflix.iceberg.types.Type;

import java.util.Collection;

/**
 * Represents a collection of one or more values of type {@link T} as a {@link Literal}.
 *
 * @param <T> The type of elements in the collection
 */
public interface CollectionLiteral<T> extends Literal<T> {
  /**
   * @return the value wrapped by this literal
   */
  Collection<T> values();

  /**
   * @return the value wrapped by this literal, as a collection of literals
   */
  Collection<ValueLiteral<T>> literalValues();

  /**
   * Converts this literal to a literal of the given type.
   * <p>
   * When a predicate is bound to a concrete data column, literals are converted to match the bound
   * column's type. This conversion process is more narrow than a cast and is only intended for
   * cases where substituting one type is a common mistake (e.g. 34 instead of 34L) or where this
   * API avoids requiring a concrete class (e.g., dates).
   * <p>
   * If conversion to a target type is not supported, this method returns null.
   * <p>
   * This method may return {@link Literals#aboveMax} or {@link Literals#belowMin} when the target
   * type is not as wide as the original type. These values indicate that the containing predicate
   * can be simplified. For example, Integer.MAX_VALUE+1 converted to an int will result in
   * {@code aboveMax} and can simplify a &lt; Integer.MAX_VALUE+1 to {@link Expressions#alwaysTrue}
   *
   * @param type A primitive {@link Type}
   * @param <X>  The Java type of value the new literal contains
   * @return A literal of the given type or null if conversion was not valid
   */
  <X> CollectionLiteral<X> to(Type type);
}
