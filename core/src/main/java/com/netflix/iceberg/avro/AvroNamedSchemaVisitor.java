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
import org.apache.avro.Schema;

import java.util.LinkedList;
import java.util.List;

/**
 * Used to visit avro schemas, but additionally pass along the schema names
 * @param <T> Type returned by folding over the avro schema
 */
public abstract class AvroNamedSchemaVisitor<T> {
  public static <T> T visit(Schema schema, String schemaName, AvroNamedSchemaVisitor<T> visitor) {
    switch (schema.getType()) {
      case RECORD:
        // check to make sure this hasn't been visited before
        String name = schema.getFullName();
        Preconditions.checkState(!visitor.recordLevels.contains(name),
            "Cannot process recursive Avro record %s", name);

        visitor.recordLevels.push(name);

        List<Schema.Field> fields = schema.getFields();
        List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
        List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
        for (Schema.Field field : schema.getFields()) {
          names.add(field.name());
          results.add(visit(field.schema(), field.name(), visitor));
        }

        visitor.recordLevels.pop();

        return visitor.record(schema, names, results);

      case UNION:
        List<Schema> types = schema.getTypes();
        List<T> options = Lists.newArrayListWithExpectedSize(types.size());
        for (Schema type : types) {
          options.add(visit(type, schemaName, visitor));
        }
        return visitor.union(schema, schemaName, options);

      case ARRAY:
        return visitor.array(schema, schemaName, visit(schema.getElementType(), schemaName, visitor));

      case MAP:
        return visitor.map(schema, schemaName, visit(schema.getValueType(), schemaName, visitor));

      default:
        return visitor.primitive(schema, schemaName);
    }
  }

  protected LinkedList<String> recordLevels = Lists.newLinkedList();

  public T record(Schema record, List<String> names, List<T> fields) {
    return null;
  }

  public T union(Schema union, String schemaName, List<T> options) {
    return null;
  }

  public T array(Schema array, String schemaName, T element) {
    return null;
  }

  public T map(Schema map, String schemaName, T value) {
    return null;
  }

  public T primitive(Schema primitive, String schemaName) {
    return null;
  }
}

