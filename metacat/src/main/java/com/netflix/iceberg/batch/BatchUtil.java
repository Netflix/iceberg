/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.netflix.iceberg.batch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.dto.FieldDto;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

class BatchUtil {
  private BatchUtil() {
  }

  static Map<String, String> buildTableOptions(Map<String, String> tableOptions, Map<String, String> operationOptions) {
    // table options take precedence because they define the table. for example, the JDBC connection URL is a table
    // option. operation options can only fill in options not defined on the table.
    ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.builder();
    optionsBuilder.putAll(tableOptions);
    Sets.difference(operationOptions.entrySet(), tableOptions.entrySet())
        .forEach(optionsBuilder::put);
    return optionsBuilder.build();
  }

  static List<FieldDto> toMetacatSchema(StructType struct, Transform[] transforms) {
    Set<String> partitionFields = SparkTables.partitionFields(transforms);
    return fieldDtos(SparkSchemaUtil.convert(struct), partitionFields);
  }

  private static List<FieldDto> fieldDtos(Schema schema, Set<String> partitionFields) {
    List<FieldDto> fields = Lists.newArrayList();

    for (int i = 0; i < schema.columns().size(); i++) {
      Types.NestedField field = schema.columns().get(i);
      FieldDto fieldInfo = new FieldDto();
      fieldInfo.setPos(i);
      fieldInfo.setName(field.name());
      fieldInfo.setType(metacatType(field.type()));
      fieldInfo.setIsNullable(field.isOptional());
      fieldInfo.setPartition_key(partitionFields.contains(field.name()));

      fields.add(fieldInfo);
    }

    return fields;
  }

  private static String metacatType(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return "boolean";
      case INTEGER:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME:
        throw new UnsupportedOperationException("Metacat does not support time fields");
      case TIMESTAMP:
        return "timestamp";
      case STRING:
      case UUID:
        return "string";
      case FIXED:
      case BINARY:
        return "binary";
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) type;
        return String.format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
      case STRUCT:
        final Types.StructType structType = type.asStructType();
        final String nameToType = structType.fields().stream()
            .map(f -> String.format("%s:%s", f.name(), metacatType(f.type()))
        ).collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return String.format("array<%s>", metacatType(listType.elementType()));
      case MAP:
        final Types.MapType mapType = type.asMapType();
        return String.format("map<%s,%s>",
            metacatType(mapType.keyType()),
            metacatType(mapType.valueType()));
      default:
        throw new UnsupportedOperationException(type +" is not supported");
    }
  }

  static Closeable noopCloseable() {
    return NoopCloseable.INSTANCE;
  }

  private static class NoopCloseable implements Closeable {
    private static final NoopCloseable INSTANCE = new NoopCloseable();

    private NoopCloseable() {
    }

    @Override
    public void close() {
    }
  }

  static String sql(Expression expr) {
    Expression rewritten = ExpressionVisitors.visit(Expressions.rewriteNot(expr), RemoveNullChecks.get());
    return ExpressionVisitors.visit(rewritten, SQLVisitor.get());
  }

  private static class RemoveNullChecks extends ExpressionVisitors.ExpressionVisitor<Expression> {
    private static final RemoveNullChecks INSTANCE = new RemoveNullChecks();

    private static RemoveNullChecks get() {
      return INSTANCE;
    }

    private RemoveNullChecks() {
    }

    @Override
    public Expression alwaysTrue() {
      return Expressions.alwaysTrue();
    }

    @Override
    public Expression alwaysFalse() {
      return Expressions.alwaysFalse();
    }

    @Override
    public Expression not(Expression result) {
      throw new UnsupportedOperationException("NOT is not supported; call rewriteNot first");
    }

    @Override
    public Expression and(Expression leftResult, Expression rightResult) {
      return Expressions.and(leftResult, rightResult);
    }

    @Override
    public Expression or(Expression leftResult, Expression rightResult) {
      return Expressions.or(leftResult, rightResult);
    }

    @Override
    public <T> Expression predicate(BoundPredicate<T> pred) {
      // assume that all null checks are always true to produce an expression that is more inclusive
      // x IS [NOT] NULL AND x = 34 becomes x = 34, and
      // x IS [NOT] NULL OR x = 34 becomes true
      // rewriting to be more inclusive is okay because the full filter will be run on rows
      if (pred.op() == Expression.Operation.IS_NULL || pred.op() == Expression.Operation.NOT_NULL) {
        return Expressions.alwaysTrue();
      }
      return pred;
    }

    @Override
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      // assume that all null checks are always true to produce an expression that is more inclusive
      // x IS [NOT] NULL AND x = 34 becomes x = 34, and
      // x IS [NOT] NULL OR x = 34 becomes true
      // rewriting to be more inclusive is okay because the full filter will be run on rows
      if (pred.op() == Expression.Operation.IS_NULL || pred.op() == Expression.Operation.NOT_NULL) {
        return Expressions.alwaysTrue();
      }
      return pred;
    }
  }

  private static class SQLVisitor extends ExpressionVisitors.ExpressionVisitor<String> {
    private static final SQLVisitor INSTANCE = new SQLVisitor();

    private static SQLVisitor get() {
      return INSTANCE;
    }

    private SQLVisitor() {
    }

    @Override
    public String alwaysTrue() {
      return "true";
    }

    @Override
    public String alwaysFalse() {
      return "false";
    }

    @Override
    public String not(String result) {
      throw new UnsupportedOperationException("NOT is not supported; call rewriteNot first");
    }

    @Override
    public String and(String leftResult, String rightResult) {
      return "(" + leftResult + " AND " + rightResult + ")";
    }

    @Override
    public String or(String leftResult, String rightResult) {
      return "(" + leftResult + " OR " + rightResult + ")";
    }

    @Override
    public <T> String predicate(BoundPredicate<T> pred) {
      throw new UnsupportedOperationException("Cannot convert bound predicates to SQL");
    }

    @Override
    public <T> String predicate(UnboundPredicate<T> pred) {
      switch (pred.op()) {
        case LT:
          return pred.ref().name() + " < " + sqlString(pred.literal());
        case LT_EQ:
          return pred.ref().name() + " <= " + sqlString(pred.literal());
        case GT:
          return pred.ref().name() + " > " + sqlString(pred.literal());
        case GT_EQ:
          return pred.ref().name() + " >= " + sqlString(pred.literal());
        case EQ:
          return pred.ref().name() + " = " + sqlString(pred.literal());
        case NOT_EQ:
          return pred.ref().name() + " != " + sqlString(pred.literal());
        case STARTS_WITH:
          return pred.ref().name() + " LIKE '" + sqlString(pred.literal()) + "%'";
        case IS_NULL:
        case NOT_NULL:
          // TODO: add support to Metacat
          throw new UnsupportedOperationException("Metacat does not support IS [NOT] NULL: " + pred);
        default:
          throw new UnsupportedOperationException("Cannot convert predicate to SQL: " + pred);
      }
    }
  }

  private static String sqlString(Literal<?> lit) {
    if (lit.value() instanceof String) {
      return "'" + lit.value() + "'";
    } else if (lit.value() instanceof ByteBuffer) {
      throw new IllegalArgumentException("Cannot convert bytes to SQL literal: " + lit);
    } else {
      return lit.value().toString();
    }
  }
}
