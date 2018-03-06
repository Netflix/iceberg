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

import com.google.common.base.Preconditions;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ORC {
  private ORC() {
  }

  public static WriteBuilder write(OutputFile file) {
    return new WriteBuilder(file);
  }

  public static class WriteBuilder {
    private final OutputFile file;
    private Schema schema = null;
    private Configuration conf = null;
    private final Properties tableProperties = new Properties();
    private Map<String, byte[]> metadata = new HashMap<>();

    private WriteBuilder(OutputFile file) {
      this.file = file;
    }

    public WriteBuilder metadata(String property, String value) {
      metadata.put(property, value.getBytes(StandardCharsets.UTF_8));
      return this;
    }

    public WriteBuilder tableProperties(Properties properties) {
      tableProperties.putAll(properties);
      return this;
    }

    public WriteBuilder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public WriteBuilder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public OrcFileAppender build() {
      if (conf == null) {
        conf = new Configuration();
      }
      OrcFile.WriterOptions options =
          OrcFile.writerOptions(tableProperties, conf);
      return new OrcFileAppender(schema, file, options, metadata);
    }
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final InputFile file;
    private com.netflix.iceberg.Schema schema = null;
    private Long start = null;
    private Long length = null;
    private Configuration conf = null;

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param start the start position for this read
     * @param length the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long start, long length) {
      this.start = start;
      this.length = length;
      return this;
    }

    public ReadBuilder schema(com.netflix.iceberg.Schema schema) {
      this.schema = schema;
      return this;
    }

    public ReadBuilder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public OrcIterator build() {
      Preconditions.checkNotNull(schema, "Schema is required");
      try {
        Path path = new Path(file.location());
        if (conf == null) {
          conf = new Configuration();
        }
        Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
        List<Integer> columnIds = new ArrayList<>();
        TypeDescription orcSchema = TypeConversion.toOrc(schema, columnIds);
        Reader.Options options = reader.options();
        if (start != null) {
          options.range(start, length);
        }
        options.schema(orcSchema);
        return new OrcIterator(path, orcSchema, reader.rows(options));
      } catch (IOException e) {
        throw new RuntimeException("Can't open " + file.location(), e);
      }
    }
  }
}
