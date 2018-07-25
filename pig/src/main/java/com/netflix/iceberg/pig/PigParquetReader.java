/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.iceberg.pig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.parquet.ParquetValueReader;
import com.netflix.iceberg.parquet.ParquetValueReaders;
import com.netflix.iceberg.parquet.ParquetValueReaders.ReusableEntry;
import com.netflix.iceberg.parquet.ParquetValueReaders.StructReader;
import com.netflix.iceberg.parquet.TypeWithSchemaVisitor;
import com.netflix.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.netflix.iceberg.parquet.ParquetSchemaUtil.convert;
import static com.netflix.iceberg.parquet.ParquetSchemaUtil.hasIds;
import static com.netflix.iceberg.parquet.ParquetValueReaders.option;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PigParquetReader {
  private final ParquetValueReader reader;

  public PigParquetReader(Schema readSchema, MessageType fileSchema, Map<Integer, Object> partitionValues) {
    this.reader = buildReader(convert(readSchema, fileSchema.getName()), readSchema, partitionValues);
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<Tuple> buildReader(MessageType fileSchema, Schema expectedSchema, Map<Integer, Object> partitionValues) {

    if (hasIds(fileSchema)) {
      return (ParquetValueReader<Tuple>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new ReadBuilder(fileSchema, partitionValues));
    } else {
      return (ParquetValueReader<Tuple>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new FallbackReadBuilder(fileSchema, partitionValues));
    }
  }

  private static class FallbackReadBuilder extends ReadBuilder {
    FallbackReadBuilder(MessageType type, Map<Integer, Object> partitionValues) {
      super(type, partitionValues);
    }

    @Override
    public ParquetValueReader<?> struct(Types.StructType ignored, GroupType struct, List<ParquetValueReader<?>> fieldReaders) {
      // the expected struct is ignored because nested fields are never found when the
      List<ParquetValueReader<?>> newFields = Lists.newArrayListWithExpectedSize(
          fieldReaders.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(fieldReaders.size());
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
        newFields.add(option(fieldType, fieldD, fieldReaders.get(i)));
        types.add(fieldType);
      }

      return new TupleReader(types, newFields, partitionValues);
    }
  }

  private static class ReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
    protected final MessageType type;
    protected final Map<Integer, Object> partitionValues;

    ReadBuilder(MessageType type, Map<Integer, Object> partitionValues) {
      this.type = type;
      this.partitionValues = partitionValues;
    }

    @Override
    public ParquetValueReader<?> message(Types.StructType expected, MessageType message, List<ParquetValueReader<?>> fieldReaders) {
      return struct(expected, message.asGroupType(), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(Types.StructType expected, GroupType struct, List<ParquetValueReader<?>> fieldReaders) {
      // match the expected struct's order
      Map<Integer, ParquetValueReader<?>> readersById = Maps.newHashMap();
      Map<Integer, Type> typesById = Maps.newHashMap();
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
        int id = fieldType.getId().intValue();
        readersById.put(id, option(fieldType, fieldD, fieldReaders.get(i)));
        typesById.put(id, fieldType);
      }

      List<Types.NestedField> expectedFields = expected != null ?
          expected.fields() : ImmutableList.of();
      List<ParquetValueReader<?>> reorderedFields = Lists.newArrayListWithExpectedSize(
          expectedFields.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(expectedFields.size());
      for (Types.NestedField field : expectedFields) {
        int id = field.fieldId();
        ParquetValueReader<?> reader = readersById.get(id);
        if (reader != null) {
          reorderedFields.add(reader);
          types.add(typesById.get(id));
        } else {
          reorderedFields.add(ParquetValueReaders.nulls());
          types.add(null);
        }
      }

      return new TupleReader(types, reorderedFields, partitionValues);
    }

    @Override
    public ParquetValueReader<?> list(Types.ListType expectedList, GroupType array, ParquetValueReader<?> elementReader) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName())) - 1;

      return new ArrayReader<>(repeatedD, repeatedR, option(elementType, elementD, elementReader));
    }

    @Override
    public ParquetValueReader<?> map(Types.MapType expectedMap, GroupType map, ParquetValueReader<?> keyReader, ParquetValueReader<?> valueReader) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName())) - 1;
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName())) - 1;

      return new MapReader<>(repeatedD, repeatedR,
          option(keyType, keyD, keyReader), option(valueType, valueD, valueReader));
    }

    @Override
    public ParquetValueReader<?> primitive(com.netflix.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive) {
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
            return new ParquetValueReaders.UnboxedReader<>(desc);
          default:
            throw new UnsupportedOperationException("Unsupported type: " + primitive.getOriginalType());
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
          return new ParquetValueReaders.UnboxedReader<>(desc);
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

    protected String[] path(String name) {
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

  private static class StringReader extends ParquetValueReaders.PrimitiveReader<String> {
    StringReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public String read(String reuse) {
      Binary binary = column.nextBinary();
      ByteBuffer buffer = binary.toByteBuffer();
      if (buffer.hasArray()) {
        return new String(
            buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining(), UTF_8);
      } else {
        return new String(binary.getBytes(), UTF_8);
      }
    }
  }

  private static class BytesReader extends ParquetValueReaders.PrimitiveReader<DataByteArray> {
    BytesReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public DataByteArray read(DataByteArray reuse) {
      byte[] bytes = column.nextBinary().getBytes();
      return new DataByteArray(Arrays.copyOf(bytes, bytes.length));
    }
  }

  private static class MapReader<K, V> extends ParquetValueReaders.RepeatedKeyValueReader<Map<K, V>, Map<K, V>, K, V> {
    ReusableEntry<K, V> nullEntry = new ReusableEntry<>();

    MapReader(int definitionLevel, int repetitionLevel,
              ParquetValueReader<K> keyReader, ParquetValueReader<V> valueReader) {
      super(definitionLevel, repetitionLevel, keyReader, valueReader);
    }

    @Override
    protected Map<K, V> newMapData(Map<K, V> reuse) {
      return new LinkedHashMap<>();
    }

    @Override
    protected Map.Entry<K, V> getPair(Map<K, V> reuse) {
      return nullEntry;
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

  private static class ArrayReader<T> extends ParquetValueReaders.RepeatedReader<DataBag, DataBag, T> {
    private final BagFactory BF = BagFactory.getInstance();
    private final TupleFactory TF = TupleFactory.getInstance();

    ArrayReader(int definitionLevel, int repetitionLevel, ParquetValueReader<T> reader) {
      super(definitionLevel, repetitionLevel, reader);
    }

    @Override
    protected DataBag newListData(DataBag reuse) {
      return BF.newDefaultBag();
    }

    @Override
    protected T getElement(DataBag list) {
      return null;
    }

    @Override
    protected void addElement(DataBag bag, T element) {
      bag.add(TF.newTuple(element));
    }

    @Override
    protected DataBag buildList(DataBag bag) {
      return bag;
    }
  }

  private static class TupleReader extends StructReader<Tuple, Tuple> {
    private static final TupleFactory TF = TupleFactory.getInstance();
    private final Map<Integer, Object> partitionValues;
    private final int columns;

    protected TupleReader(List<Type> types, List<ParquetValueReader<?>> readers, Map<Integer, Object> partitionValues) {
      super(types, readers);

      this.partitionValues = partitionValues;
      this.columns = types.size() + partitionValues.size();
    }

    @Override
    protected Tuple newStructData(Tuple reuse) {
      return TF.newTuple(columns);
    }

    @Override
    protected Object getField(Tuple tuple, int pos) {
      return null;
    }

    @Override
    protected Tuple buildStruct(Tuple tuple) {
      for (Map.Entry<Integer, Object> e : partitionValues.entrySet()) {
        try {
          tuple.set(e.getKey(), e.getValue());
        } catch (ExecException ex) {
          throw new RuntimeException("", ex);
        }
      }

      return tuple;
    }

    @Override
    protected void set(Tuple tuple, int pos, Object value) {
      try {
        tuple.set(pos, value);
      } catch (ExecException e) {
        throw new RuntimeException("", e);
      }
    }

    @Override
    protected void setNull(Tuple tuple, int pos) {
      try {
        tuple.set(pos, null);
      } catch (ExecException e) {
        throw new RuntimeException("", e);
      }
    }

    @Override
    protected void setBoolean(Tuple tuple, int pos, boolean value) {
      try {
        tuple.set(pos, value);
      } catch (ExecException e) {
        throw new RuntimeException("", e);
      }
    }

    @Override
    protected void setInteger(Tuple tuple, int pos, int value) {
      try {
        tuple.set(pos, value);
      } catch (ExecException e) {
        throw new RuntimeException("", e);
      }
    }

    @Override
    protected void setLong(Tuple tuple, int pos, long value) {
      try {
        tuple.set(pos, value);
      } catch (ExecException e) {
        throw new RuntimeException("", e);
      }
    }

    @Override
    protected void setFloat(Tuple tuple, int pos, float value) {
      try {
        tuple.set(pos, value);
      } catch (ExecException e) {
        throw new RuntimeException("", e);
      }
    }

    @Override
    protected void setDouble(Tuple tuple, int pos, double value) {
      try {
        tuple.set(pos, value);
      } catch (ExecException e) {
        throw new RuntimeException("", e);
      }
    }
  }
}
