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

package com.netflix.iceberg.parquet;

import com.netflix.iceberg.Schema;
import com.netflix.iceberg.expressions.*;
import com.netflix.iceberg.expressions.Expression.Operation;
import com.netflix.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import com.netflix.iceberg.types.Types;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.netflix.iceberg.expressions.ExpressionVisitors.visit;

class ParquetFilters {

  static FilterCompat.Filter convert(Schema schema, Expression expr) {
    FilterPredicate pred = visit(expr, new ConvertFilterToParquet(schema));
    // TODO: handle AlwaysFalse.INSTANCE
    if (pred != null && pred != AlwaysTrue.INSTANCE) {
      // FilterCompat will apply LogicalInverseRewriter
      return FilterCompat.get(pred);
    } else {
      return FilterCompat.NOOP;
    }
  }

  static FilterCompat.Filter convertColumnFilter(Schema schema, String column, Expression expr) {
    FilterPredicate pred = visit(expr, new ConvertColumnFilterToParquet(schema, column));
    // TODO: handle AlwaysFalse.INSTANCE
    if (pred != null && pred != AlwaysTrue.INSTANCE) {
      // FilterCompat will apply LogicalInverseRewriter
      return FilterCompat.get(pred);
    } else {
      return FilterCompat.NOOP;
    }
  }

  private static class ConvertFilterToParquet extends ExpressionVisitor<FilterPredicate> {
    private final Schema schema;

    private ConvertFilterToParquet(Schema schema) {
      this.schema = schema;
    }

    @Override
    public FilterPredicate alwaysTrue() {
      return AlwaysTrue.INSTANCE;
    }

    @Override
    public FilterPredicate alwaysFalse() {
      return AlwaysFalse.INSTANCE;
    }

    @Override
    public FilterPredicate not(FilterPredicate child) {
      if (child == AlwaysTrue.INSTANCE) {
        return AlwaysFalse.INSTANCE;
      } else if (child == AlwaysFalse.INSTANCE) {
        return AlwaysTrue.INSTANCE;
      }
      return FilterApi.not(child);
    }

    @Override
    public FilterPredicate and(FilterPredicate left, FilterPredicate right) {
      if (left == AlwaysFalse.INSTANCE || right == AlwaysFalse.INSTANCE) {
        return AlwaysFalse.INSTANCE;
      } else if (left == AlwaysTrue.INSTANCE) {
        return right;
      } else if (right == AlwaysTrue.INSTANCE) {
        return left;
      }
      return FilterApi.and(left, right);
    }

    @Override
    public FilterPredicate or(FilterPredicate left, FilterPredicate right) {
      if (left == AlwaysTrue.INSTANCE || right == AlwaysTrue.INSTANCE) {
        return AlwaysTrue.INSTANCE;
      } else if (left == AlwaysFalse.INSTANCE) {
        return right;
      } else if (right == AlwaysFalse.INSTANCE) {
        return left;
      }
      return FilterApi.or(left, right);
    }

    @Override
    public <T> FilterPredicate predicate(BoundPredicate<T, ?> predicate) {
      Operation op = predicate.op();
      BoundReference<T> ref = predicate.ref();
      Literal<T> lit = predicate.literal();
      String path = schema.idToAlias(ref.fieldId());

      switch (ref.type().typeId()) {
        case BOOLEAN:
          Operators.BooleanColumn col = FilterApi.booleanColumn(schema.idToAlias(ref.fieldId()));
          switch (op) {
            case EQ:
              return FilterApi.eq(col, getParquetPrimitive(lit));
            case NOT_EQ:
              return FilterApi.eq(col, getParquetPrimitive(lit));
          }

        case INTEGER:
          return pred(predicate, FilterApi.intColumn(path));
        case LONG:
          return pred(predicate, FilterApi.longColumn(path));
        case FLOAT:
          return pred(predicate, FilterApi.floatColumn(path));
        case DOUBLE:
          return pred(predicate, FilterApi.doubleColumn(path));
        case DATE:
          return pred(predicate, FilterApi.intColumn(path));
        case TIME:
          return pred(predicate, FilterApi.longColumn(path));
        case TIMESTAMP:
          return pred(predicate, FilterApi.longColumn(path));
        case STRING:
          return pred(predicate, FilterApi.binaryColumn(path));
        case UUID:
          return pred(predicate, FilterApi.binaryColumn(path));
        case FIXED:
          return pred(predicate, FilterApi.binaryColumn(path));
        case BINARY:
          return pred(predicate, FilterApi.binaryColumn(path));
        case DECIMAL:
          return pred(predicate, FilterApi.binaryColumn(path));
      }

      throw new UnsupportedOperationException("Cannot convert to Parquet filter: " + predicate);
    }

    protected Expression bind(UnboundPredicate<?, ?> pred) {
      return pred.bind(schema.asStruct());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> FilterPredicate predicate(UnboundPredicate<T, ?> pred) {
      Expression bound = bind(pred);
      if (bound instanceof BoundPredicate) {
        return predicate((BoundPredicate<?, ?>) bound);
      } else if (bound == Expressions.alwaysTrue()) {
        return AlwaysTrue.INSTANCE;
      } else if (bound == Expressions.alwaysFalse()) {
        return AlwaysFalse.INSTANCE;
      }
      throw new UnsupportedOperationException("Cannot convert to Parquet filter: " + pred);
    }
  }

  private static class ConvertColumnFilterToParquet extends ConvertFilterToParquet {
    private final Types.StructType partitionStruct;

    private ConvertColumnFilterToParquet(Schema schema, String column) {
      super(schema);
      this.partitionStruct = schema.findField(column).type().asNestedType().asStructType();
    }

    protected Expression bind(UnboundPredicate<?, ?> pred) {
      // instead of binding the predicate using the top-level schema, bind it to the partition data
      return pred.bind(partitionStruct);
    }
  }

  private static
  <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsLtGt>
  FilterPredicate pred(BoundPredicate<?, ?> predicate, COL col) {
    if (predicate instanceof BoundUnaryPredicate) {
      return pred(predicate.op(), col);
    }

    if (predicate instanceof BoundValuePredicate) {
      ValueLiteral<?> literal = ((BoundValuePredicate<?>) predicate).literal();
      return pred(predicate.op(), col, ParquetFilters.<C>getParquetPrimitive(literal.value()));
    }
    
    if (predicate instanceof BoundCollectionPredicate) {
      CollectionLiteral<?> literal = ((BoundCollectionPredicate<?>) predicate).literal();
      Collection<C> values = literal.values().stream()
        .map(ParquetFilters::<C>getParquetPrimitive)
        .collect(Collectors.toList());
      return pred(predicate.op(), col, values);
    }

    throw new UnsupportedOperationException("Unsupported type of bound predicate: " + predicate.getClass().getName());
  }

  private static
    <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsEqNotEq>
    FilterPredicate pred(Operation op, COL col) {

    switch (op) {
      case IS_NULL:
        return FilterApi.eq(col, null);
      case NOT_NULL:
        return FilterApi.notEq(col, null);
      default:
        throw new UnsupportedOperationException("Unsupported unary predicate operation: " + op);
    }
  }

  private static
  <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsLtGt>
  FilterPredicate pred(Operation op, COL col, C value) {
    switch (op) {
      case EQ:
        return FilterApi.eq(col, value);
      case NOT_EQ:
        return FilterApi.notEq(col, value);
      case GT:
        return FilterApi.gt(col, value);
      case GT_EQ:
        return FilterApi.gtEq(col, value);
      case LT:
        return FilterApi.lt(col, value);
      case LT_EQ:
        return FilterApi.ltEq(col, value);
      default:
        throw new UnsupportedOperationException("Unsupported value predicate operation: " + op);
    }
  }

  private static
  <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsLtGt>
  FilterPredicate pred(Operation op, COL col, Collection<C> values) {
    switch (op) {
      case IN:
        return values.stream()
          .map(value -> (FilterPredicate)FilterApi.eq(col, value))
          .reduce(FilterApi::or)
          .orElse(AlwaysFalse.INSTANCE);
      case NOT_IN:
        return FilterApi.not(
          values.stream()
            .map(value -> (FilterPredicate) FilterApi.eq(col, value))
            .reduce(FilterApi::or)
            .orElse(AlwaysFalse.INSTANCE)
        );
      default:
        throw new UnsupportedOperationException("Unsupported collection predicate operation: " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private static <C extends Comparable<C>> C getParquetPrimitive(Object value) {
    // TODO: this needs to convert to handle BigDecimal and UUID
    if (value instanceof Number) {
      return (C) value;
    } else if (value instanceof CharSequence) {
      return (C) Binary.fromString(value.toString());
    } else if (value instanceof ByteBuffer) {
      return (C) Binary.fromReusedByteBuffer((ByteBuffer) value);
    }
    throw new UnsupportedOperationException(
      "Type not supported yet: " + value.getClass().getName());
  }

  private static class AlwaysTrue implements FilterPredicate {
    static final AlwaysTrue INSTANCE = new AlwaysTrue();

    @Override
    public <R> R accept(Visitor<R> visitor) {
      throw new UnsupportedOperationException("AlwaysTrue is a placeholder only");
    }
  }

  private static class AlwaysFalse implements FilterPredicate {
    static final AlwaysFalse INSTANCE = new AlwaysFalse();

    @Override
    public <R> R accept(Visitor<R> visitor) {
      throw new UnsupportedOperationException("AlwaysTrue is a placeholder only");
    }
  }
}
