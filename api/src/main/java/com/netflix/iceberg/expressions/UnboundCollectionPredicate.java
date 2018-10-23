package com.netflix.iceberg.expressions;

import com.google.common.base.Joiner;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.types.Types;

import java.util.Collection;

/**
 * This class represents a predicate that has not been bound to a known field yet and is expected to compare it to a
 * collection of that field's type.
 *
 * @param <T> The type of values in the field
 */
public class UnboundCollectionPredicate<T> extends UnboundPredicate<T, CollectionLiteral<T>> {
  UnboundCollectionPredicate(Operation op, NamedReference namedRef, Collection<T> values) {
    super(op, namedRef, new Literals.CollectionLiteralImpl<>(values));
  }

  UnboundCollectionPredicate(Operation op, NamedReference namedRef, CollectionLiteral<T> lit) {
    super(op, namedRef, lit);
  }

  @Override
  public Expression negate() {
    return new UnboundCollectionPredicate<>(op().negate(), ref(), literal());
  }

  public Expression bind(Types.StructType struct) {
    Types.NestedField field = struct.field(ref().name());
    ValidationException.check(field != null,
      "Cannot find field '%s' in struct: %s", ref().name(), struct);
    ValidationException.check(literal() != null, "Value can not be null");

    switch (op()) {
      case IN:
      case NOT_IN:
        break;
      default:
        throw new ValidationException("Operation must be IN or NOT_IN");
    }

    return new BoundCollectionPredicate<>(
      op(),
      new BoundReference<>(struct, field.fieldId()),
      literal()
    );
  }
}