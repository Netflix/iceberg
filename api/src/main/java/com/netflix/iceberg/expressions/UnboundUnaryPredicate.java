package com.netflix.iceberg.expressions;

import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.types.Types;

import static com.netflix.iceberg.expressions.Expression.Operation.IS_NULL;
import static com.netflix.iceberg.expressions.Expression.Operation.NOT_NULL;

/**
 * This class represents a predicate that has not been bound to a known field yet and is expected to execute a unary
 * action on it (doesn't need right-hand-side), like is-null or not-null.
 *
 * @param <T> The type of values in the field
 */
public class UnboundUnaryPredicate<T> extends UnboundPredicate<T, Literal<T>> {
  UnboundUnaryPredicate(Operation op, NamedReference namedRef) {
    super(op, namedRef, null);
  }

  @Override
  public Expression negate() {
    return new UnboundUnaryPredicate<>(op().negate(), ref());
  }

  public Expression bind(Types.StructType struct) {
    Types.NestedField field = struct.field(ref().name());
    ValidationException.check(field != null,
      "Cannot find field '%s' in struct: %s", ref().name(), struct);

    switch (op()) {
      case IS_NULL:
        if (field.isRequired()) {
          return Expressions.alwaysFalse();
        }
        return new BoundUnaryPredicate<>(IS_NULL, new BoundReference<>(struct, field.fieldId()));
      case NOT_NULL:
        if (field.isRequired()) {
          return Expressions.alwaysTrue();
        }
        return new BoundUnaryPredicate<>(NOT_NULL, new BoundReference<>(struct, field.fieldId()));
      default:
        throw new ValidationException("Operation must be IS_NULL or NOT_NULL");
    }
  }
}