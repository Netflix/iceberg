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

package org.apache.iceberg.spark.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class MetadataColumns {
  private MetadataColumns() {
  }

  static final IcebergMetadataColumn FILE_PATH = new IcebergMetadataColumn(
      Integer.MAX_VALUE - 1, "_file", Types.StringType.get(), "Path of the file in which a row is stored");
  static final IcebergMetadataColumn ROW_POSITION = new IcebergMetadataColumn(
      Integer.MAX_VALUE - 2, "_pos", Types.LongType.get(), "Ordinal position of a row in the source data file");

  private static final Map<String, IcebergMetadataColumn> META_COLUMNS = ImmutableMap.of(FILE_PATH.name(), FILE_PATH);

  static boolean nonMetadataColumn(String name) {
    return !META_COLUMNS.containsKey(name);
  }

  static boolean isMetadataColumn(String name) {
    return META_COLUMNS.containsKey(name);
  }

  static MetadataColumn get(String name) {
    return META_COLUMNS.get(name);
  }

  static StructType addMetadataColumns(StructType struct, String[] metaColumns) {
    return new StructType(Streams.concat(
        Stream.of(struct.fields()),
        Stream.of(metaColumns)
            .map(MetadataColumns::get)
            .map(MetadataColumns::toStructField)
    ).toArray(StructField[]::new));
  }

  private static StructField toStructField(MetadataColumn metaColumn) {
    return new StructField(metaColumn.name(), metaColumn.dataType(), true, Metadata.empty())
        .withComment(metaColumn.comment());
  }

  static Types.StructType toStruct(List<MetadataColumn> metaColumns) {
    return Types.StructType.of(metaColumns.stream()
        .map(MetadataColumns::toNestedField)
        .toArray(Types.NestedField[]::new));
  }

  private static Types.NestedField toNestedField(MetadataColumn metaColumn) {
    Preconditions.checkArgument(metaColumn instanceof IcebergMetadataColumn,
        "Unsupported metadata column: %s", toString(metaColumn));
    return ((IcebergMetadataColumn) metaColumn).asNestedField();
  }

  private static String toString(MetadataColumn metaColumn) {
    return String.format("%s: %s %s%s",
        metaColumn.name(), metaColumn.isNullable() ? "optional" : "required", metaColumn.dataType().simpleString(),
        metaColumn.comment() != null ? " (" + metaColumn.comment() + ")" : "");
  }

  private static class IcebergMetadataColumn implements MetadataColumn {
    private final int id;
    private final String name;
    private final Type type;
    private final DataType sparkType;
    private final String comment;
    private final Types.NestedField asNestedField;

    private IcebergMetadataColumn(int id, String name, Type type, String comment) {
      this.id = id;
      this.name = name;
      this.type = type;
      this.sparkType = SparkSchemaUtil.convert(type);
      this.comment = comment;
      this.asNestedField = Types.NestedField.of(id, true, name, type, comment);
    }

    public int id() {
      return id;
    }

    @Override
    public String name() {
      return name;
    }

    public Type type() {
      return type;
    }

    @Override
    public DataType dataType() {
      return sparkType;
    }

    @Override
    public String comment() {
      return comment;
    }

    @Override
    public Transform transform() {
      return null;
    }

    public Types.NestedField asNestedField() {
      return asNestedField;
    }
  }
}
