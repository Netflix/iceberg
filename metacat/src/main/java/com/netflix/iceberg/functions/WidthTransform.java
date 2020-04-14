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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.Serializable;
import java.math.BigDecimal;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class WidthTransform implements UnboundFunction {
  private final Identifier ident;

  public WidthTransform(Identifier ident) {
    this.ident = ident;
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.size() > 2) {
      throw new UnsupportedOperationException("Cannot apply transform to more than one argument");
    } else if (inputType.size() < 2) {
      throw new UnsupportedOperationException("Two arguments are required");
    }

    DataType argType = inputType.fields()[0].dataType();
    Type icebergType = SparkSchemaUtil.convert(argType);

    if (!(inputType.fields()[1].dataType() instanceof IntegerType)) {
      throw new UnsupportedOperationException("Second argument must be an integer");
    }

    if (ident.name().equalsIgnoreCase("bucket")) {
      return buildTransform(
          ident.toString(), width -> Transforms.bucket(icebergType, width), argType, DataTypes.IntegerType);
    } else if (ident.name().equalsIgnoreCase("truncate")) {
      DataType resultType = SparkSchemaUtil.convert(Transforms.truncate(icebergType, 4).getResultType(icebergType));
      return buildTransform(ident.toString(), width -> Transforms.truncate(icebergType, width), argType, resultType);
    }

    throw new UnsupportedOperationException("Unknown transform: " + ident);
  }

  @Override
  public String name() {
    return ident.toString();
  }

  private interface TransformFactory<S, T> extends Serializable {
    Transform<S, T> get(int width);
  }

  private static <S, T> BoundTransform<S, T> buildTransform(
      String name, TransformFactory<S, T> buildTransform, DataType inputType, DataType resultType) {
    if (inputType instanceof StringType) {
      return new StringTransform<>(name, buildTransform, resultType);
    } else if (inputType instanceof DecimalType) {
      return new DecimalTransform<>(name, buildTransform, inputType, resultType);
    } else {
      return new BoundTransform<>(name, buildTransform, inputType, resultType);
    }
  }

  private static class StringTransform<S, T> extends BoundTransform<S, T> {
    public StringTransform(String name, TransformFactory<S, T> buildTransform, DataType resultType) {
      super(name, buildTransform, DataTypes.StringType, resultType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public S fromSpark(Object value) {
      return (S) ((UTF8String) value).toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T toSpark(T value) {
      if (value instanceof String) {
        return (T) UTF8String.fromString((String) value);
      } else {
        return value;
      }
    }
  }

  private static class DecimalTransform<S, T> extends BoundTransform<S, T> {
    public DecimalTransform(String name, TransformFactory<S, T> buildTransform,
                            DataType decimalType, DataType resultType) {
      super(name, buildTransform, decimalType, resultType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public S fromSpark(Object value) {
      return (S) ((Decimal) value).toJavaBigDecimal();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T toSpark(T value) {
      if (value instanceof BigDecimal) {
        return (T) Decimal.apply((BigDecimal) value);
      } else {
        return value;
      }
    }
  }

  private static class BoundTransform<S, T> implements ScalarFunction<T> {
    private final String name;
    private final TransformFactory<S, T> buildTransform;
    private final DataType inputType;
    private final DataType resultType;
    private transient LoadingCache<Integer, Transform<S, T>> transformCache = null;

    public BoundTransform(String name, TransformFactory<S, T> buildTransform,
                          DataType inputType, DataType resultType) {
      this.name = name;
      this.buildTransform = buildTransform;
      this.inputType = inputType;
      this.resultType = resultType;
    }

    public Transform<S, T> transformFor(int width) {
      if (transformCache == null) {
        this.transformCache = Caffeine.newBuilder().build(buildTransform::get);
      }
      return transformCache.get(width);
    }

    @SuppressWarnings("unchecked")
    public S fromSpark(Object value) {
      return (S) value;
    }

    public T toSpark(T value) {
      return value;
    }

    @Override
    public T produceResult(InternalRow input) {
      if (input.isNullAt(0)) {
        return null;
      }

      Transform<S, T> transform = transformFor(input.getInt(1));

      return toSpark(transform.apply(fromSpark(input.get(0, inputType))));
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
}
