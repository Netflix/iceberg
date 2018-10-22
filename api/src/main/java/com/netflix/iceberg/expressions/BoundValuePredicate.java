package com.netflix.iceberg.expressions;

/**
 * This class represents a predicate that has been bound to a known field and will be comparing it to a value of
 * that field's type.
 *
 * @param <T> The type of values in the field
 */
public class BoundValuePredicate<T> extends BoundPredicate<T, ValueLiteral<T>> {
  BoundValuePredicate(Operation op, BoundReference<T> ref, ValueLiteral<T> lit) {
    super(op, ref, lit);
  }

  @Override
  public Expression negate() {
    return new BoundValuePredicate<>(op().negate(), ref(), literal());
  }

  public UnboundValuePredicate<T> unbind(String newName) {
    return new UnboundValuePredicate<>(op(), new NamedReference(newName), literal());
  }
}
