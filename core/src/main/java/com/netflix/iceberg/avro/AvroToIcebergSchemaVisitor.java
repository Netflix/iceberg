package com.netflix.iceberg.avro;

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;

import java.util.HashMap;
import java.util.List;

public class AvroToIcebergSchemaVisitor extends AvroNamedSchemaVisitor<com.netflix.iceberg.Schema> {

  private org.apache.avro.Schema rootSchema;

  AvroToIcebergSchemaVisitor(org.apache.avro.Schema rootSchema) {
    this.rootSchema = rootSchema;
    if (rootSchema.getType() == org.apache.avro.Schema.Type.RECORD) {
      nextId = rootSchema.getFields().size();
    }
  }

  private int nextId = 1;

  private int allocateId() {
    int current = nextId;
    nextId += 1;
    return current;
  }

  @Override
  public Schema record(org.apache.avro.Schema record, List<String> names, List<Schema> fields) {
    List<org.apache.avro.Schema.Field> avroFields = record.getFields();
    List<Types.NestedField> nestedFields = Lists.newArrayListWithExpectedSize(avroFields.size());
    HashMap<String, Integer> idToAlias = Maps.newHashMap();

    if (rootSchema == record) {
      this.nextId = 0;
    }

    for (int i = 0; i < fields.size(); i++) {
      org.apache.avro.Schema.Field avroField = avroFields.get(i);
      Schema icebergSchema = fields.get(i);
      String fieldName = names.get(i);

      Types.NestedField field;
      if (avroField.schema().getType() == org.apache.avro.Schema.Type.RECORD) {
        // If this is a record type, take field and put it inside a struct
        int fieldId = allocateId();
        field =
            AvroSchemaUtil.isOptionSchema(avroField.schema()) ?
                Types.NestedField.optional(fieldId, fieldName, Types.StructType.of(icebergSchema.columns())) :
                Types.NestedField.required(fieldId, fieldName, Types.StructType.of(icebergSchema.columns()));
        idToAlias.put(fieldName, fieldId);
      } else if (record == rootSchema) {
        // if we're on the root, allocate fresh ids (don't use the field ids generated in the previous iteration)
        int fieldId = allocateId();
        Type fieldType = AvroSchemaUtil.convert(avroField.schema());

        field =
            AvroSchemaUtil.isOptionSchema(avroField.schema()) ?
                Types.NestedField.optional(fieldId, fieldName, fieldType) :
                Types.NestedField.required(fieldId, fieldName, fieldType);

        idToAlias.put(fieldName, fieldId);
      } else {
        // Otherwise, just take the ids already defined in the previous run
        field = icebergSchema.findField(fieldName);
        idToAlias.put(fieldName, field.fieldId());
      }

      nestedFields.add(field);
    }

    return new Schema(nestedFields, idToAlias);
  }

  @Override
  public Schema array(org.apache.avro.Schema array, String schemaName, Schema element) {
    org.apache.avro.Schema elementSchema = array.getElementType();
    HashMap<String, Integer> aliasToId = Maps.newHashMap();

    Type icebergType = AvroSchemaUtil.convert(elementSchema);

    int fieldId = allocateId();

    Types.ListType listType =
        AvroSchemaUtil.isOptionSchema(elementSchema) ?
            Types.ListType.ofOptional(fieldId, icebergType) :
            Types.ListType.ofRequired(fieldId, icebergType);

    Types.NestedField listField =
        listType.isElementOptional() ?
            Types.NestedField.optional(listType.elementId(), schemaName, listType) :
            Types.NestedField.required(listType.elementId(), schemaName, listType);

    aliasToId.put(schemaName, fieldId);

    return new Schema(Lists.newArrayList(listField), aliasToId);
  }

  @Override
  public Schema map(org.apache.avro.Schema map, String schemaName, Schema value) {
    org.apache.avro.Schema valueSchema = map.getValueType();

    int keyId = allocateId();
    int valueId = allocateId();

    Type valueIcebergSchema = AvroSchemaUtil.convert(valueSchema);

    // Avro only allows for maps with String as key
    Type keyType = Types.StringType.get();

    Types.MapType mapType = AvroSchemaUtil.isOptionSchema(valueSchema) ?
        Types.MapType.ofOptional(keyId, valueId, keyType, valueIcebergSchema) :
        Types.MapType.ofRequired(keyId, valueId, keyType, valueIcebergSchema);

    Types.NestedField mapField = AvroSchemaUtil.isOptionSchema(map) ?
        Types.NestedField.optional(nextId, schemaName, mapType) :
        Types.NestedField.required(nextId, schemaName, mapType);

    return new Schema(mapField);
  }

  @Override
  public Schema primitive(org.apache.avro.Schema primitive, String schemaName) {
    if (primitive.getType() == org.apache.avro.Schema.Type.NULL) return null;

    HashMap<String, Integer> aliasToId = Maps.newHashMap();

    Type primitiveType = AvroSchemaUtil.convert(primitive);

    int fieldId = allocateId();

    Types.NestedField primitiveField =
        AvroSchemaUtil.isOptionSchema(primitive) ?
            Types.NestedField.optional(fieldId, schemaName, primitiveType) :
            Types.NestedField.required(fieldId, schemaName, primitiveType);

    aliasToId.put(schemaName, fieldId);
    return new Schema(Lists.newArrayList(primitiveField), aliasToId);
  }

  @Override
  public Schema union(org.apache.avro.Schema union, String schemaName, List<Schema> options) {
    Preconditions.checkArgument(AvroSchemaUtil.isOptionSchema(union),
        "Unsupported type: non-option union: {}", union);
    // records, arrays, and maps will check nullability later

    HashMap<String, Integer> aliasToId = Maps.newHashMap();

    aliasToId.put(schemaName, nextId);

    int fieldId = options.get(1).findField(schemaName).fieldId();
    Type primitiveType = AvroSchemaUtil.convert(union);

    return new Schema(Lists.newArrayList(Types.NestedField.optional(fieldId, schemaName, primitiveType)), aliasToId);
  }
}
