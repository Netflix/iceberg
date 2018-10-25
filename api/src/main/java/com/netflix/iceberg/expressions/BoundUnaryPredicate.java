package com.netflix.iceberg.expressions;

/**
 * This class represents a predicate that has been bound to a known field and will be executing a unary
 * action on it (doesn't need right-hand-side), like is-null or not-null.
 *
 * @param <T> The type of values in the field
 */
public class BoundUnaryPredicate<T> extends BoundPredicate<T, Literal<T>> {
  BoundUnaryPredicate(Operation op, BoundReference<T> ref) {
    super(op, ref, null);
  }

  @Override
  public Expression negate() {
    return new BoundUnaryPredicate<>(op().negate(), ref());
  }

  public UnboundUnaryPredicate<T> typedUnbind(String newName) {
    return this.unbind(newName);
  }

  public <S> UnboundUnaryPredicate<S> unbind(String newName) {
    return new UnboundUnaryPredicate<>(op(), new NamedReference(newName));
  }
}
