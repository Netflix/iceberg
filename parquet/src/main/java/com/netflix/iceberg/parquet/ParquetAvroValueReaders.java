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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.parquet.ParquetValueReaders.RepeatedKeyValueReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.RepeatedReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.StructReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.UnboxedReader;
import com.netflix.iceberg.types.Types;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.netflix.iceberg.parquet.ParquetValueReaders.option;

public class ParquetAvroValueReaders {
  private ParquetAvroValueReaders() {
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<Record> buildReader(MessageType readSchema,
                                                       com.netflix.iceberg.Schema schema) {
    return (ParquetValueReader<Record>) ParquetTypeVisitor
        .visit(readSchema, new ReadBuilder(readSchema, schema));
  }

  private static class ReadBuilder extends ParquetTypeVisitor<ParquetValueReader<?>> {
    private final MessageType type;
    private final com.netflix.iceberg.Schema schema;
    private final Map<com.netflix.iceberg.types.Type, Schema> avroSchemas;

    ReadBuilder(MessageType type, com.netflix.iceberg.Schema schema) {
      this.type = type;
      this.schema = schema;
      this.avroSchemas = AvroSchemaUtil.convertTypes(schema.asStruct(), type.getName());
    }

    @Override
    public ParquetValueReader<?> message(MessageType message,
                                         List<ParquetValueReader<?>> fieldReaders) {
      return struct(message.asGroupType(), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(GroupType struct,
                                        List<ParquetValueReader<?>> fieldReaders) {
      Schema avroSchema;
      if (struct == type) {
        avroSchema = avroSchemas.get(schema.asStruct());
      } else {
        int fieldId = struct.getId().intValue();
        Types.NestedField field = schema.findField(fieldId);
        avroSchema = avroSchemas.get(field.type());
      }
      int structD = type.getMaxDefinitionLevel(currentPath());
      return new RecordReader(struct, structD, fieldReaders, avroSchema);
    }

    @Override
    public ParquetValueReader<?> list(GroupType array, ParquetValueReader<?> elementReader) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath)-1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath)-1;

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName()));

      return new ListReader<>(repeatedD, repeatedR, option(elementType, elementD, elementReader));
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

      boolean isMapKey = fieldNames.contains("key");

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            if (isMapKey) {
              return new StringReader(desc);
            }
            return new Utf8Reader(desc);
          case DATE:
          case INT_8:
          case INT_16:
          case INT_32:
          case INT_64:
          case TIME_MICROS:
          case TIMESTAMP_MICROS:
            return new UnboxedReader<>(desc);
          case TIME_MILLIS:
            return new TimeMillisReader(desc);
          case TIMESTAMP_MILLIS:
            return new TimestampMillisReader(desc);
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new DecimalReader(desc, decimal.getScale());
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
          int fieldId = primitive.getId().intValue();
          Schema avroSchema = AvroSchemaUtil.convert(schema.findType(fieldId));
          return new FixedReader(desc, avroSchema);
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

  private static class TimeMillisReader extends UnboxedReader<Long> {
    TimeMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public long readLong() {
      return 1000 * column.nextLong();
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

  static class DecimalReader extends ParquetValueReaders.PrimitiveReader<BigDecimal> {
    private final int scale;

    DecimalReader(ColumnDescriptor desc, int scale) {
      super(desc);
      this.scale = scale;
    }

    @Override
    public BigDecimal read() {
      return new BigDecimal(new BigInteger(column.nextBinary().getBytesUnsafe()), scale);
    }
  }

  static class StringReader extends ParquetValueReaders.PrimitiveReader<String> {
    StringReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public String read() {
      return column.nextBinary().toStringUsingUTF8();
    }
  }

  static class Utf8Reader extends ParquetValueReaders.PrimitiveReader<Utf8> {
    Utf8Reader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Utf8 read() {
      return new Utf8(column.nextBinary().getBytes());
    }
  }

  static class UUIDReader extends ParquetValueReaders.PrimitiveReader<UUID> {
    UUIDReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public UUID read() {
      ByteBuffer buffer = column.nextBinary().toByteBuffer();
      buffer.order(ByteOrder.BIG_ENDIAN);

      long mostSigBits = buffer.getLong();
      long leastSigBits = buffer.getLong();

      return new UUID(mostSigBits, leastSigBits);
    }
  }

  static class FixedReader extends ParquetValueReaders.PrimitiveReader<Fixed> {
    private final Schema schema;

    FixedReader(ColumnDescriptor desc, Schema schema) {
      super(desc);
      this.schema = schema;
    }

    @Override
    public Fixed read() {
      Fixed fixed = new Fixed(schema);
      fixed.bytes(column.nextBinary().getBytes());
      return fixed;
    }
  }

  static class BytesReader extends ParquetValueReaders.PrimitiveReader<ByteBuffer> {
    BytesReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public ByteBuffer read() {
      return column.nextBinary().toByteBuffer();
    }
  }

  static class ListReader<E> extends RepeatedReader<List<E>, List<E>, E> {
    ListReader(int definitionLevel, int repetitionLevel,
                      ParquetValueReader<E> reader) {
      super(definitionLevel, repetitionLevel, reader);
    }

    @Override
    protected List<E> newListData() {
      return Lists.newArrayList();
    }

    @Override
    protected void addElement(List<E> list, E element) {
      list.add(element);
    }

    @Override
    protected List<E> buildList(List<E> list) {
      return list;
    }
  }

  static class MapReader<K, V> extends RepeatedKeyValueReader<Map<K, V>, Map<K, V>, K, V> {
    MapReader(int definitionLevel, int repetitionLevel,
                        ParquetValueReader<K> keyReader,
                        ParquetValueReader<V> valueReader) {
      super(definitionLevel, repetitionLevel, keyReader, valueReader);
    }

    @Override
    protected Map<K, V> newMapData() {
      return Maps.newLinkedHashMap();
    }

    @Override
    protected void addPair(Map<K, V> map, K key, V value) {
      map.put(key, value);
    }

    @Override
    protected Map<K, V> buildMap(Map<K, V> map) {
      return map;
    }
  }

  static class RecordReader extends StructReader<Record, Record> {
    private final Schema schema;

    RecordReader(GroupType type, int definitionLevel,
                        List<ParquetValueReader<?>> readers,
                        Schema schema) {
      super(type, definitionLevel, readers);
      this.schema = schema;
    }

    @Override
    protected Record newStructData(GroupType type) {
      return new Record(schema);
    }

    @Override
    protected Record buildStruct(Record struct) {
      return struct;
    }

    @Override
    protected void set(Record struct, int pos, Object value) {
      struct.put(pos, value);
    }
  }
}
