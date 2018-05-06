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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.parquet.ParquetTypeVisitor;
import com.netflix.iceberg.parquet.ParquetValueReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.PrimitiveReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.RepeatedKeyValueReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.RepeatedReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.ReusableEntry;
import com.netflix.iceberg.parquet.ParquetValueReaders.StructReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.UnboxedReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.netflix.iceberg.parquet.ParquetSchemaUtil.convert;
import static com.netflix.iceberg.parquet.ParquetValueReaders.option;

public class SparkParquetReader {
  private final ParquetValueReader reader;
  private final UnsafeRowWriter rowWriter;

  public SparkParquetReader(Schema readSchema, MessageType fileSchema) {
    // use the read schema to build the reader so that field order is correct.
    // TODO: this will break if required fields in the file are optional in the read schema
    this.reader = buildReader(convert(readSchema, fileSchema.getName()), readSchema);
    this.rowWriter = null; // new UnsafeRowWriter();
  }

  @SuppressWarnings("unchecked")
  private static ParquetValueReader<UnsafeRow> buildReader(MessageType type, Schema schema) {
    return (ParquetValueReader<UnsafeRow>) ParquetTypeVisitor
        .visit(type, new ReadBuilder(type, schema));
  }

  private static class ReadBuilder extends ParquetTypeVisitor<ParquetValueReader<?>> {
    private final MessageType type;
    private final Schema schema;

    ReadBuilder(MessageType type, Schema schema) {
      this.type = type;
      this.schema = schema;
    }

    @Override
    public ParquetValueReader<?> message(MessageType message,
                                         List<ParquetValueReader<?>> fieldReaders) {
      return struct(message.asGroupType(), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(GroupType struct,
                                        List<ParquetValueReader<?>> fieldReaders) {
      int structD = type.getMaxDefinitionLevel(currentPath());
      return new InternalRowReader(struct, structD, fieldReaders);
    }

    @Override
    public ParquetValueReader<?> list(GroupType array, ParquetValueReader<?> elementReader) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath)-1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath)-1;

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName()));

      return new ArrayReader<>(repeatedD, repeatedR, option(elementType, elementD, elementReader));
    }

    @Override
    public ParquetValueReader<?> map(GroupType map,
                                     ParquetValueReader<?> keyReader,
                                     ParquetValueReader<?> valueReader) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath)-1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath)-1;

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName()));
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName()));

      return new MapReader<>(repeatedD, repeatedR,
          option(keyType, keyD, keyReader), option(valueType, valueD, valueReader));
    }

    @Override
    public ParquetValueReader<?> primitive(PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return new StringReader(desc);
          case DATE:
          case INT_8:
          case INT_16:
          case INT_32:
          case INT_64:
          case TIMESTAMP_MICROS:
            return new UnboxedReader<>(desc);
          case TIMESTAMP_MILLIS:
            return new TimestampMillisReader(desc);
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new BinaryDecimalReader(desc, decimal.getScale());
              case INT64:
                return new LongDecimalReader(desc, decimal.getPrecision(), decimal.getScale());
              case INT32:
                return new IntegerDecimalReader(desc, decimal.getPrecision(), decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return new BytesReader(desc);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return new BytesReader(desc);
        case BOOLEAN:
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
          return new UnboxedReader<>(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }

    private String[] currentPath() {
      String[] path = new String[fieldNames.size()];
      if (!fieldNames.isEmpty()) {
        Iterator<String> iter = fieldNames.descendingIterator();
        for (int i = 0; iter.hasNext(); i += 1) {
          path[i] = iter.next();
        }
      }

      return path;
    }

    private String[] path(String name) {
      String[] path = new String[fieldNames.size() + 1];
      path[fieldNames.size()] = name;

      if (!fieldNames.isEmpty()) {
        Iterator<String> iter = fieldNames.descendingIterator();
        for (int i = 0; iter.hasNext(); i += 1) {
          path[i] = iter.next();
        }
      }

      return path;
    }
  }

  private static class BinaryDecimalReader extends PrimitiveReader<Decimal> {
    private final int scale;

    BinaryDecimalReader(ColumnDescriptor desc, int scale) {
      super(desc);
      this.scale = scale;
    }

    @Override
    public Decimal read(Decimal ignored) {
      Binary binary = column.nextBinary();
      return Decimal.fromDecimal(new BigDecimal(new BigInteger(binary.getBytes()), scale));
    }
  }

  private static class IntegerDecimalReader extends PrimitiveReader<Decimal> {
    private final int precision;
    private final int scale;

    IntegerDecimalReader(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Decimal read(Decimal ignored) {
      return Decimal.apply(column.nextInteger(), precision, scale);
    }
  }

  private static class LongDecimalReader extends PrimitiveReader<Decimal> {
    private final int precision;
    private final int scale;

    LongDecimalReader(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Decimal read(Decimal ignored) {
      return Decimal.apply(column.nextInteger(), precision, scale);
    }
  }

  private static class TimestampMillisReader extends UnboxedReader<Long> {
    TimestampMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public long readLong() {
      return 1000 * column.nextLong();
    }
  }

  private static class StringReader extends PrimitiveReader<UTF8String> {
    StringReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public UTF8String read(UTF8String ignored) {
      Binary binary = column.nextBinary();
      ByteBuffer buffer = binary.toByteBuffer();
      if (buffer.hasArray()) {
        return UTF8String.fromBytes(
            buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      } else {
        return UTF8String.fromBytes(binary.getBytes());
      }
    }
  }

  private static class BytesReader extends PrimitiveReader<byte[]> {
    BytesReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public byte[] read(byte[] ignored) {
      return column.nextBinary().getBytes();
    }
  }

  private static class ArrayReader<E> extends RepeatedReader<ArrayData, Void, E> {
    private final List<E> reusedList = Lists.newArrayList();
    private Iterator<E> elements = null;

    ArrayReader(int definitionLevel, int repetitionLevel, ParquetValueReader<E> reader) {
      super(definitionLevel, repetitionLevel, reader);
      // TODO: create a new GenericArrayData implementation that can be reused
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Void newListData(ArrayData reuse) {
      reusedList.clear();

      if (reuse instanceof GenericArrayData) {
        this.elements = Iterators.forArray((E[]) reuse.array());
      } else {
        this.elements = Iterators.emptyIterator();
      }

      return null;
    }

    @Override
    protected E getElement(Void list) {
      if (elements.hasNext()) {
        return elements.next();
      }

      return null;
    }

    @Override
    protected void addElement(Void ignored, E element) {
      reusedList.add(element);
    }

    @Override
    protected ArrayData buildList(Void list) {
      return new GenericArrayData(reusedList.toArray());
    }
  }

  private static class MapReader<K, V> extends RepeatedKeyValueReader<MapData, Void, K, V> {
    private final List<Object> reusedKeyList = Lists.newArrayList();
    private final List<Object> reusedValueList = Lists.newArrayList();
    private final ReusableEntry<K, V> entry = new ReusableEntry<>();
    private final ReusableEntry<K, V> nullEntry = new ReusableEntry<>();

    private Iterator<K> keys = null;
    private Iterator<V> values = null;

    MapReader(int definitionLevel, int repetitionLevel,
              ParquetValueReader<K> keyReader, ParquetValueReader<V> valueReader) {
      super(definitionLevel, repetitionLevel, keyReader, valueReader);
      nullEntry.set(null, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Void newMapData(MapData reuse) {
      if (reuse instanceof ArrayBasedMapData) {
        this.keys = Iterators.forArray((K[]) reuse.keyArray().array());
        this.values = Iterators.forArray((V[]) reuse.valueArray().array());
      } else {
        this.keys = Iterators.emptyIterator();
        this.values = Iterators.emptyIterator();
      }

      return null;
    }

    @Override
    protected Map.Entry<K, V> getPair(Void map) {
      if (keys.hasNext()) {
        entry.set(keys.next(), values.next());
        return entry;
      } else {
        return nullEntry;
      }
    }

    @Override
    protected void addPair(Void map, K key, V value) {
      reusedKeyList.add(key);
      reusedValueList.add(value);
    }

    @Override
    protected MapData buildMap(Void map) {
      return new ArrayBasedMapData(
          new GenericArrayData(reusedKeyList.toArray()),
          new GenericArrayData(reusedValueList.toArray()));
    }
  }

  private static class InternalRowReader extends StructReader<InternalRow, GenericInternalRow> {
    private final int numFields;

    InternalRowReader(GroupType type, int definitionLevel, List<ParquetValueReader<?>> readers) {
      super(type, definitionLevel, readers);
      this.numFields = readers.size();
    }

    @Override
    protected GenericInternalRow newStructData(GroupType type, InternalRow reuse) {
      if (reuse != null && reuse instanceof GenericInternalRow) {
        return (GenericInternalRow) reuse;
      } else {
        return new GenericInternalRow(numFields);
      }
    }

    @Override
    protected Object getField(GenericInternalRow intermediate, int pos) {
      return intermediate.genericGet(pos);
    }

    @Override
    protected InternalRow buildStruct(GenericInternalRow struct) {
      return struct;
    }

    @Override
    protected void set(GenericInternalRow row, int pos, Object value) {
      row.update(pos, value);
    }

    @Override
    protected void setNull(GenericInternalRow row, int pos) {
      row.setNullAt(pos);
    }

    @Override
    protected void setBoolean(GenericInternalRow row, int pos, boolean value) {
      row.setBoolean(pos, value);
    }

    @Override
    protected void setInteger(GenericInternalRow row, int pos, int value) {
      row.setInt(pos, value);
    }

    @Override
    protected void setLong(GenericInternalRow row, int pos, long value) {
      row.setLong(pos, value);
    }

    @Override
    protected void setFloat(GenericInternalRow row, int pos, float value) {
      row.setFloat(pos, value);
    }

    @Override
    protected void setDouble(GenericInternalRow row, int pos, double value) {
      row.setDouble(pos, value);
    }
  }
}
