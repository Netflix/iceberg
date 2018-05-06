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

import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ParquetReader<T> implements Iterable<T>, Closeable {
  private final InputFile input;
  private final ParquetValueReader<T> model;
  private ParquetFileReader reader = null;

  public ParquetReader(InputFile input, ParquetValueReader<T> reader) throws IOException {
    this.input = input;
    this.model = reader;
    this.reader = ParquetFileReader.open(ParquetIO.file(input));
  }

  @Override
  public Iterator<T> iterator() {
    return new FileIterator<>(reader, model);
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      this.reader = null;
    }
  }

  private static class FileIterator<T> implements Iterator<T> {
    private final ParquetFileReader reader;
    private final List<BlockMetaData> rowGroups;
    private final long totalValues;
    private final ParquetValueReader<T> model;

    private long nextRowGroupStart = 0;
    private long valuesRead = 0;
    private T last = null;

    public FileIterator(ParquetFileReader reader, ParquetValueReader<T> model) {
      this.reader = reader;
      this.rowGroups = reader.getRowGroups();
      this.totalValues = reader.getRecordCount();
      this.model = model;
    }

    @Override
    public boolean hasNext() {
      return valuesRead < totalValues;
    }

    @Override
    public T next() {
      if (valuesRead >= nextRowGroupStart) {
        advance();
      }

      this.last = model.read(last);
      valuesRead += 1;

      return last;
    }

    private void advance() {
      PageReadStore pages;
      try {
        pages = reader.readNextRowGroup();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      nextRowGroupStart += pages.getRowCount();

      model.setPageSource(pages);
    }
  }
}
