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

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.connector.catalog.BoundFunction;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.ScalarFunction;
import org.apache.spark.sql.connector.catalog.UnboundFunction;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class ReadManifestDataFilePaths implements UnboundFunction {
  private static final ArrayData EMPTY = new GenericArrayData(new Object[0]);

  private final Identifier ident;
  private final FileIO io;

  public ReadManifestDataFilePaths(Identifier ident) {
    this.ident = ident;
    this.io = new HadoopFileIO(SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration());
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.size() > 1) {
      throw new UnsupportedOperationException("Cannot apply transform to more than one argument");
    } else if (inputType.size() < 1) {
      throw new UnsupportedOperationException("At least one argument is required");
    }

    DataType argType = inputType.fields()[0].dataType();
    if (!(argType instanceof StringType)) {
      throw new UnsupportedOperationException("Cannot apply %s to non-string type: " + argType.simpleString());
    }

    return new BoundManifestRead(ident.toString(), io);
  }

  @Override
  public String name() {
    return ident.toString();
  }

  private static class BoundManifestRead implements ScalarFunction<ArrayData> {
    private final String name;
    private final FileIO io;

    private BoundManifestRead(String name, FileIO io) {
      this.name = name;
      this.io = io;
    }

    @Override
    public ArrayData produceResult(InternalRow input) {
      if (input.isNullAt(0)) {
        return EMPTY;
      }

      String manifestPath = input.getString(0);
      try (CloseableIterable<DataFile> files = ManifestReader.read(io.newInputFile(manifestPath)).select("file_path")) {
        List<UTF8String> paths = Lists.newArrayList(Iterables.transform(files,
            file -> UTF8String.fromString(file.path().toString())));
        return new GenericArrayData(paths.toArray(new UTF8String[0]));
      } catch (IOException e) {
        throw new RuntimeIOException("Failed to read manifest: " + manifestPath);
      }
    }

    @Override
    public DataType resultType() {
      return new ArrayType(DataTypes.StringType, false /* does not contain null */);
    }

    @Override
    public String name() {
      return name;
    }
  }
}
