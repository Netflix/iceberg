package com.netflix.iceberg.expressions;

import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.types.Types;

import java.util.function.Function;

import static com.netflix.iceberg.expressions.Expression.Operation.*;

/**
 * This class represents a predicate that has not been bound to a known field yet and is expected to compare it to a
 * value of that field's type.
 *
 * @param <T> The type of values in the field
 */
public class UnboundValuePredicate<T> extends UnboundPredicate<T, ValueLiteral<T>> {
  UnboundValuePredicate(Operation op, NamedReference namedRef, T value) {
    super(op, namedRef, Literals.from(value));
  }

  UnboundValuePredicate(Operation op, NamedReference namedRef, ValueLiteral<T> lit) {
    super(op, namedRef, lit);
  }

  @Override
  public Expression negate() {
    return new UnboundValuePredicate<>(op().negate(), ref(), literal());
  }

  public Expression bind(Types.StructType struct) {
    Types.NestedField field = struct.field(ref().name());
    ValidationException.check(field != null,
      "Cannot find field '%s' in struct: %s", ref().name(), struct);
    ValidationException.check(literal() != null, "Value can not be null");
    // TODO: We need to find a single location to map which op is allowed by which predicate type
    ValidationException.check(op() != IN && op() != NOT_IN && op() != IS_NULL && op() != NOT_NULL,
      "The right hand side of the predicate must exist and be a single value");

    ValueLiteral<T> lit = literal().to(field.type());

    if (lit == null) {
      throw new ValidationException(String.format(
        "Invalid value for comparison inclusive type %s: %s (%s)",
        field.type(), literal().value(), literal().value().getClass().getName()));

    } else if (lit == Literals.aboveMax()) {
      switch (op()) {
        case LT:
        case LT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case GT:
        case GT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
      }
    } else if (lit == Literals.belowMin()) {
      switch (op()) {
        case GT:
        case GT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case LT:
        case LT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
      }
    }

    return new BoundValuePredicate<>(op(), new BoundReference<>(struct, field.fieldId()), lit);
  }

  public <S> UnboundValuePredicate<S> transform(Function<T, S> f) {
    return new UnboundValuePredicate<>(op(), ref(), f.apply(literal().value()));
  }

}