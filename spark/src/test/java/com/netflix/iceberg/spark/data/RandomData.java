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

package com.netflix.iceberg.spark.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.TypeUtil;
import com.netflix.iceberg.types.Types;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

public class RandomData {
  public static List<Record> generate(Schema schema, int numRecords, long seed) {
    RandomDataGenerator generator = new RandomDataGenerator(schema, seed);
    List<Record> records = Lists.newArrayListWithExpectedSize(numRecords);
    for (int i = 0; i < numRecords; i += 1) {
      records.add((Record) TypeUtil.visit(schema, generator));
    }

    return records;
  }

  private static class RandomDataGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Map<Type, org.apache.avro.Schema> typeToSchema;
    private final Random random;

    private RandomDataGenerator(Schema schema, long seed) {
      this.typeToSchema = AvroSchemaUtil.convertTypes(schema.asStruct(), "test");
      this.random = new Random(seed);
    }

    @Override
    public Record schema(Schema schema, Supplier<Object> structResult) {
      return (Record) structResult.get();
    }

    @Override
    public Record struct(Types.StructType struct, Iterable<Object> fieldResults) {
      Record rec = new Record(typeToSchema.get(struct));

      List<Object> values = Lists.newArrayList(fieldResults);
      for (int i = 0; i < values.size(); i += 1) {
        rec.put(i, values.get(i));
      }

      return rec;
    }

    @Override
    public Object field(Types.NestedField field, Supplier<Object> fieldResult) {
      // return null 5% of the time when the value is optional
      if (field.isOptional() && random.nextInt(20) == 1) {
        return null;
      }
      return fieldResult.get();
    }

    @Override
    public Object list(Types.ListType list, Supplier<Object> elementResult) {
      int numElements = random.nextInt(20);

      List<Object> result = Lists.newArrayListWithExpectedSize(numElements);
      for (int i = 0; i < numElements; i += 1) {
        // return null 5% of the time when the value is optional
        if (list.isElementOptional() && random.nextInt(20) == 1) {
          result.add(null);
        } else {
          result.add(elementResult.get());
        }
      }

      return result;
    }

    @Override
    public Object map(Types.MapType map, Supplier<Object> valueResult) {
      int numEntries = random.nextInt(20);

      Map<String, Object> result = Maps.newLinkedHashMap();
      for (int i = 0; i < numEntries; i += 1) {
        String key = randomString(random).toString() + i; // add i to ensure no collisions
        // return null 5% of the time when the value is optional
        if (map.isValueOptional() && random.nextInt(20) == 1) {
          result.put(key, null);
        } else {
          result.put(key, valueResult.get());
        }
      }

      return result;
    }

    @Override
    public Object primitive(Type.PrimitiveType primitive) {
      Object result = generatePrimitive(primitive, random);
      // For the primitives that Avro needs a different type than Spark, fix
      // them here.
      switch (primitive.typeId()) {
        case STRING:
          return ((UTF8String) result).toString();
        case FIXED:
          return new GenericData.Fixed(typeToSchema.get(primitive),
              (byte[]) result);
        case BINARY:
          return ByteBuffer.wrap((byte[]) result);
        case UUID:
          return UUID.nameUUIDFromBytes((byte[]) result);
        case DECIMAL:
          return ((Decimal) result).toJavaBigDecimal();
        default:
          return result;
      }
    }
  }

  public static Object generatePrimitive(Type.PrimitiveType primitive,
                                         Random random) {
    int choice = random.nextInt(20);

    switch (primitive.typeId()) {
      case BOOLEAN:
        return choice < 10;

      case INTEGER:
        switch (choice) {
          case 1:
            return Integer.MIN_VALUE;
          case 2:
            return Integer.MAX_VALUE;
          case 3:
            return 0;
          default:
            return random.nextInt();
        }

      case LONG:
        switch (choice) {
          case 1:
            return Long.MIN_VALUE;
          case 2:
            return Long.MAX_VALUE;
          case 3:
            return 0L;
          default:
            return random.nextLong();
        }

      case FLOAT:
        switch (choice) {
          case 1:
            return Float.MIN_VALUE;
          case 2:
            return -Float.MIN_VALUE;
          case 3:
            return Float.MAX_VALUE;
          case 4:
            return -Float.MAX_VALUE;
          case 5:
            return Float.NEGATIVE_INFINITY;
          case 6:
            return Float.POSITIVE_INFINITY;
          case 7:
            return 0.0F;
          case 8:
            return Float.NaN;
          default:
            return random.nextFloat();
        }

      case DOUBLE:
        switch (choice) {
          case 1:
            return Double.MIN_VALUE;
          case 2:
            return -Double.MIN_VALUE;
          case 3:
            return Double.MAX_VALUE;
          case 4:
            return -Double.MAX_VALUE;
          case 5:
            return Double.NEGATIVE_INFINITY;
          case 6:
            return Double.POSITIVE_INFINITY;
          case 7:
            return 0.0D;
          case 8:
            return Double.NaN;
          default:
            return random.nextDouble();
        }

      case DATE:
        // this will include negative values (dates before 1970-01-01)
        return random.nextInt() % ABOUT_380_YEARS_IN_DAYS;

      case TIME:
        return (random.nextLong() & Integer.MAX_VALUE) % ONE_DAY_IN_MICROS;

      case TIMESTAMP:
        return random.nextLong() % FIFTY_YEARS_IN_MICROS;

      case STRING:
        return randomString(random);

      case UUID:
        byte[] uuidBytes = new byte[16];
        random.nextBytes(uuidBytes);
        // this will hash the uuidBytes
        return uuidBytes;

      case FIXED:
        byte[] fixed = new byte[((Types.FixedType) primitive).length()];
        random.nextBytes(fixed);
        return fixed;

      case BINARY:
        byte[] binary = new byte[random.nextInt(50)];
        random.nextBytes(binary);
        return binary;

      case DECIMAL:
        Types.DecimalType type = (Types.DecimalType) primitive;
        BigInteger unscaled = randomUnscaled(type.precision(), random);
        return Decimal.apply(new BigDecimal(unscaled, type.scale()));

      default:
        throw new IllegalArgumentException(
            "Cannot generate random value for unknown type: " + primitive);
    }
  }

  private static final long FIFTY_YEARS_IN_MICROS =
      (50L * (365 * 3 + 366) * 24 * 60 * 60 * 1_000_000) / 4;
  private static final int ABOUT_380_YEARS_IN_DAYS = 380 * 365;
  private static final long ONE_DAY_IN_MICROS = 24 * 60 * 60 * 1_000_000L;
  private static final String CHARS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?";

  private static UTF8String randomString(Random random) {
    int length = random.nextInt(50);
    byte[] buffer = new byte[length];

    for (int i = 0; i < length; i += 1) {
      buffer[i] = (byte) CHARS.charAt(random.nextInt(CHARS.length()));
    }

    return UTF8String.fromBytes(buffer);
  }

  private static final String DIGITS = "0123456789";
  private static BigInteger randomUnscaled(int precision, Random random) {
    int length = random.nextInt(precision);
    if (length == 0) {
      return BigInteger.ZERO;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i += 1) {
      sb.append(DIGITS.charAt(random.nextInt(DIGITS.length())));
    }

    return new BigInteger(sb.toString());
  }

  public static Iterator<InternalRow> generateSpark(Schema schema,
                                                    int rows,
                                                    long seed) {
    return new Iterator<InternalRow>(){
      private int rowsLeft = rows;
      SparkGenerator generator = buildGenerator(schema.asStruct(),
          new Random(seed), false);

      @Override
      public boolean hasNext() {
        return rowsLeft > 0;
      }

      @Override
      public InternalRow next() {
        rowsLeft -= 1;
        return (InternalRow) generator.next();
      }
    };
  }

  interface SparkGenerator {
    /**
     * Generate the next object for Spark.
     * @return InternalRow, MapData, ArrayData, etc.
     */
    Object next();
  }

  /**
   * A filter that generates a null 5% of the time.
   */
  static class OptionalSparkGenerator implements  SparkGenerator {
    private final SparkGenerator child;
    private final Random random;

    OptionalSparkGenerator(SparkGenerator child, Random random) {
      this.child = child;
      this.random = random;
    }

    @Override
    public Object next() {
      return random.nextInt(100) < 5 ? null : child.next();
    }
  }

  static class PrimitiveSparkGenerator implements SparkGenerator {
    private final Type.PrimitiveType type;
    private final Random random;

    PrimitiveSparkGenerator(Type type, Random random) {
      this.type = (Type.PrimitiveType) type;
      this.random = random;
    }

    @Override
    public Object next() {
      return generatePrimitive(type, random);
    }
  }

  static class StructSparkGenerator implements SparkGenerator {
    private final SparkGenerator[] children;

    StructSparkGenerator(Type type, Random random) {
      Types.StructType t = (Types.StructType) type;
      List<Types.NestedField> fields = t.fields();
      children = new SparkGenerator[fields.size()];
      for(int c=0; c < children.length; ++c) {
        Types.NestedField field = fields.get(c);
        children[c] = buildGenerator(field.type(), random, field.isOptional());
      }
    }

    @Override
    public Object next() {
      GenericInternalRow row = new GenericInternalRow(children.length);
      for(int c=0; c < children.length; ++c) {
        row.update(c, children[c].next());
      }
      return row;
    }
  }

  static class ListSparkGenerator implements SparkGenerator {
    private final Random random;
    private final SparkGenerator child;

    ListSparkGenerator(Type type, Random random) {
      this.random = random;
      Types.ListType t = (Types.ListType) type;
      child = buildGenerator(t.elementType(), random, t.isElementOptional());
    }

    @Override
    public Object next() {
      int len = random.nextInt(20);
      GenericArrayData result = new GenericArrayData(new Object[len]);
      for(int e=0; e < len; ++e) {
        result.update(e, child.next());
      }
      return result;
    }
  }

  static class MapSparkGenerator implements SparkGenerator {
    private final Random random;
    private final SparkGenerator keyGenerator;
    private final SparkGenerator valueGenerator;

    MapSparkGenerator(Type type, Random random) {
      this.random = random;
      Types.MapType t = (Types.MapType) type;
      keyGenerator = buildGenerator(t.keyType(), random, false);
      valueGenerator = buildGenerator(t.valueType(), random, t.isValueOptional());
    }

    @Override
    public Object next() {
      int len = random.nextInt(20);
      GenericArrayData keys = new GenericArrayData(new Object[len]);
      GenericArrayData values = new GenericArrayData(new Object[len]);
      ArrayBasedMapData result = new ArrayBasedMapData(keys, values);
      List alreadyUsed = new ArrayList(len);
      for(int e=0; e < len; ++e) {
        Object key;
        do {
          key = keyGenerator.next();
        } while (alreadyUsed.contains(key));
        alreadyUsed.add(key);
        keys.update(e, key);
        values.update(e, valueGenerator.next());
      }
      return result;
    }
  }

  static SparkGenerator buildGenerator(Type type, Random random,
                                       boolean isOptional) {
    SparkGenerator result;
    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case STRING:
      case UUID:
      case FIXED:
      case BINARY:
      case DECIMAL:
        result = new PrimitiveSparkGenerator(type, random);
        break;
      case STRUCT:
        result = new StructSparkGenerator(type, random);
        break;
      case LIST:
        result = new ListSparkGenerator(type, random);
        break;
      case MAP:
        result = new MapSparkGenerator(type, random);
        break;
      default:
        throw new IllegalArgumentException("Unhandled type " + type);
    }
    return isOptional ? new OptionalSparkGenerator(result, random) : result;
  }
}
