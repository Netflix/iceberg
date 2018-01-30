/*
 * Copyright 2018 Hortonworks
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

package com.netflix.iceberg.orc;

import com.netflix.iceberg.Schema;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;

public class TypeConversion {

  /**
   * Convert a given Iceberg schema to ORC.
   * @param schema the Iceberg schema to convert
   * @param columnIds an output with the column ids
   * @return the ORC schema
   */
  public static TypeDescription toOrc(Schema schema,
                                      List<Integer> columnIds) {
    columnIds.add(0);
    return toOrc(schema.asStruct(), columnIds);
  }

  static TypeDescription toOrc(Type type, List<Integer> columnIds) {
    switch (type.typeId()) {
      case BOOLEAN:
        return TypeDescription.createBoolean();
      case INTEGER:
        return TypeDescription.createInt();
      case LONG:
        return TypeDescription.createLong();
      case FLOAT:
        return TypeDescription.createFloat();
      case DOUBLE:
        return TypeDescription.createDouble();
      case DATE:
        return TypeDescription.createDate();
      case TIME:
        return TypeDescription.createInt();
      case TIMESTAMP:
        return TypeDescription.createTimestamp();
      case STRING:
        return TypeDescription.createString();
      case UUID:
        return TypeDescription.createBinary();
      case FIXED:
        return TypeDescription.createBinary();
      case BINARY:
        return TypeDescription.createBinary();
      case DECIMAL: {
        Types.DecimalType decimal = (Types.DecimalType) type;
        return TypeDescription.createDecimal()
            .withScale(decimal.scale())
            .withPrecision(decimal.precision());
      }
      case STRUCT: {
        TypeDescription struct = TypeDescription.createStruct();
        for(Types.NestedField field: type.asStructType().fields()) {
          columnIds.add(field.fieldId());
          struct.addField(field.name(), toOrc(field.type(), columnIds));
        }
        return struct;
      }
      case LIST: {
        Types.ListType list = (Types.ListType) type;
        columnIds.add(list.elementId());
        return TypeDescription.createList(toOrc(type.asListType().elementType(),
            columnIds));
      }
      case MAP: {
        Types.MapType map = (Types.MapType) type;
        columnIds.add(map.keyId());
        TypeDescription key = toOrc(map.keyType(), columnIds);
        columnIds.add(map.valueId());
        return TypeDescription.createMap(key,
            toOrc(type.asMapType().valueType(), columnIds));
      }
      default:
        throw new IllegalArgumentException("Unhandled type " + type.typeId());
    }
  }

  /**
   * Convert an ORC schema to an Iceberg schema.
   * @param schema the ORC schema
   * @param columnIds the column ids
   * @return the Iceberg schema
   */
  public Schema fromOrc(TypeDescription schema, int[] columnIds) {
    return new Schema(convertOrcToType(schema, columnIds).asStructType().fields());
  }

  Type convertOrcToType(TypeDescription schema, int[] columnIds) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case BYTE:
      case SHORT:
      case INT:
        return Types.IntegerType.get();
      case LONG:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case STRING:
      case CHAR:
      case VARCHAR:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      case DATE:
        return Types.DateType.get();
      case TIMESTAMP:
        return Types.TimestampType.withoutZone();
      case DECIMAL:
        return Types.DecimalType.of(schema.getPrecision(), schema.getScale());
      case STRUCT: {
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> fieldTypes = schema.getChildren();
        List<Types.NestedField> fields = new ArrayList<>(fieldNames.size());
        for(int c=0; c < fieldNames.size(); ++c) {
          String name = fieldNames.get(c);
          TypeDescription type = fieldTypes.get(c);
          fields.add(Types.NestedField.optional(columnIds[type.getId()], name,
              convertOrcToType(type, columnIds)));
        }
        return Types.StructType.of(fields);
      }
      case LIST: {
        TypeDescription child = schema.getChildren().get(0);
        return Types.ListType.ofOptional(columnIds[child.getId()],
            convertOrcToType(child, columnIds));
      }
      case MAP: {
        TypeDescription key = schema.getChildren().get(0);
        TypeDescription value = schema.getChildren().get(1);
        switch (key.getCategory()) {
          case STRING:
          case CHAR:
          case VARCHAR:
            return Types.MapType.ofOptional(columnIds[key.getId()],
                columnIds[value.getId()], convertOrcToType(value, columnIds));
          default:
            throw new IllegalArgumentException("Can't handle maps with " + key +
                " as key.");
        }
      }
      default:
        // We don't have an answer for union types -> blech.
        throw new IllegalArgumentException("Can't handle " + schema);
    }
  }
}
