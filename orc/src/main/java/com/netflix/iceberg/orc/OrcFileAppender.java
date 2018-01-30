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
 */package com.netflix.iceberg.orc;

import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.OutputFile;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create a file appender for ORC.
 */
public class OrcFileAppender implements FileAppender<VectorizedRowBatch> {
  private final Writer writer;
  private final TypeDescription orcSchema;
  private final List<Integer> columnIds = new ArrayList<>();
  private final Path path;

  public static final String COLUMN_NUMBERS_ATTRIBUTE = "iceberg.column.ids";

  static ByteBuffer buidIdString(List<Integer> list) {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < list.size(); ++i) {
      if (i != 0) {
        buffer.append(',');
      }
      buffer.append(list.get(i));
    }
    return ByteBuffer.wrap(buffer.toString().getBytes(StandardCharsets.UTF_8));
  }

  OrcFileAppender(Schema schema,
                  PartitionSpec spec,
                  OutputFile file,
                  OrcFile.WriterOptions options,
                  Map<String,byte[]> metadata) {
    orcSchema = TypeConversion.toOrc(schema, columnIds);
    options.setSchema(orcSchema);
    path = new Path(file.location());
    try {
      writer = OrcFile.createWriter(path, options);
    } catch (IOException e) {
      throw new RuntimeException("Can't create file " + path, e);
    }
    writer.addUserMetadata(COLUMN_NUMBERS_ATTRIBUTE, buidIdString(columnIds));
    metadata.forEach(
        (key,value) -> writer.addUserMetadata(key, ByteBuffer.wrap(value)));
  }

  @Override
  public void add(VectorizedRowBatch datum) {
    try {
      writer.addRowBatch(datum);
    } catch (IOException e) {
      throw new RuntimeException("Problem writing to ORC file " + path, e);
    }
  }

  @Override
  public Metrics metrics() {
    try {
      long rows = writer.getNumberOfRows();
      ColumnStatistics[] stats = writer.getStatistics();
      // we don't currently have columnSizes or distinct counts.
      Map<Integer, Long> valueCounts = new HashMap<>();
      Map<Integer, Long> nullCounts = new HashMap<>();
      for(int c=1; c < stats.length; ++c) {
        int fieldId = columnIds.get(c);
        valueCounts.put(fieldId, stats[c].getNumberOfValues());
      }
      for(TypeDescription child: orcSchema.getChildren()) {
        int c = child.getId();
        int fieldId = columnIds.get(c);
        nullCounts.put(fieldId, rows - stats[c].getNumberOfValues());
      }
      return new Metrics(rows, null, valueCounts, nullCounts);
    } catch (IOException e) {
      throw new RuntimeException("Can't get statistics " + path, e);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public TypeDescription getSchema() {
    return orcSchema;
  }
}
