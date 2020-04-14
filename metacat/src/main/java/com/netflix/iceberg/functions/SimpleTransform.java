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

package com.netflix.iceberg.functions;

import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.BoundFunction;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.ScalarFunction;
import org.apache.spark.sql.connector.catalog.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public class SimpleTransform implements UnboundFunction {
  private final Identifier ident;

  public SimpleTransform(Identifier ident) {
    this.ident = ident;
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.size() > 1) {
      throw new UnsupportedOperationException("Cannot apply transform to more than one argument");
    } else if (inputType.size() < 1) {
      throw new UnsupportedOperationException("At least one argument is required");
    }

    DataType operandType = inputType.fields()[0].dataType();
    Type icebergType = SparkSchemaUtil.convert(operandType);

    Transform<?, ?> transform = transform(ident.name(), icebergType, operandType.simpleString());
    if (!transform.canTransform(icebergType)) {
      throw new UnsupportedOperationException(String.format(
          "%s cannot transform %s values", ident, operandType.simpleString()));
    }

    DataType resultType = SparkSchemaUtil.convert(transform.getResultType(icebergType));

    return new BoundTransform<>(ident.toString(), transform, inputType, resultType);
  }

  @Override
  public String name() {
    return ident.toString();
  }

  private static class BoundTransform<S, T> implements ScalarFunction<T> {
    private final String name;
    private final Transform<S, T> transform;
    private final DataType inputType;
    private final DataType resultType;

    public BoundTransform(String name, Transform<S, T> transform, DataType inputType, DataType resultType) {
      this.name = name;
      this.transform = transform;
      this.inputType = inputType;
      this.resultType = resultType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T produceResult(InternalRow input) {
      if (input.isNullAt(0)) {
        return null;
      } else {
        return transform.apply((S) input.get(0, inputType));
      }
    }

    @Override
    public DataType resultType() {
      return resultType;
    }

    @Override
    public String name() {
      return name;
    }
  }

  private static Transform<?, ?> transform(String name, Type inputType, String typeString) {
    try {
      switch (name) {
        case "identity":
          return Transforms.identity(inputType);
        case "year":
        case "years":
          return Transforms.year(inputType);
        case "month":
        case "months":
          return Transforms.month(inputType);
        case "day":
        case "days":
        case "date":
          return Transforms.day(inputType);
        case "hour":
        case "hours":
        case "date_hour":
        case "date_and_hour":
          return Transforms.hour(inputType);
      }
    } catch (IllegalArgumentException e) {
      throw new UnsupportedOperationException(String.format(
          "%s cannot transform %s values", name, typeString));
    }

    throw new UnsupportedOperationException(String.format(
        "Unknown transform: %s(%s)", name, typeString));
  }
}
