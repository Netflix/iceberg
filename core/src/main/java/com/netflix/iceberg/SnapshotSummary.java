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

package com.netflix.iceberg;

import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import java.util.Map;

public class SnapshotSummary {
  public static final String GENIE_ID_PROP = "genie-id";
  public static final String ADDED_FILES_PROP = "added-data-files";
  public static final String DELETED_FILES_PROP = "deleted-data-files";
  public static final String TOTAL_FILES_PROP = "total-data-files";
  public static final String ADDED_RECORDS_PROP = "added-records";
  public static final String DELETED_RECORDS_PROP = "deleted-records";
  public static final String TOTAL_RECORDS_PROP = "total-records";
  public static final String ADDED_FILE_SIZE_PROP = "added-files-size";
  public static final String DELETED_DUPLICATE_FILES = "deleted-duplicate-files";
  public static final String CHANGED_PARTITION_COUNT_PROP = "changed-partition-count";
  public static final String CHANGED_PARTITION_PREFIX = "partitions.";
  public static final String PARTITION_SUMMARY_PROP = "partition-summaries-included";
  public static final MapJoiner MAP_JOINER = Joiner.on(",").withKeyValueSeparator("=");
  public static final MapSplitter MAP_SPLITTER = Splitter.on(",").withKeyValueSeparator("=");

  private SnapshotSummary() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    // commit summary tracking
    private Map<String, ScanSummary.PartitionMetrics> changedPartitions = Maps.newHashMap();
    private long addedFiles = 0L;
    private long deletedFiles = 0L;
    private long deletedDuplicateFiles = 0L;
    private long addedRecords = 0L;
    private long deletedRecords = 0L;

    public void clear() {
      changedPartitions.clear();
      this.addedFiles = 0L;
      this.deletedFiles = 0L;
      this.deletedDuplicateFiles = 0L;
      this.addedRecords = 0L;
      this.deletedRecords = 0L;
    }

    public void incrementDuplicateDeletes() {
      this.deletedDuplicateFiles += 1;
    }

    public void deletedFile(PartitionSpec spec, DataFile file) {
      updatePartitions(spec, file, false);
      this.deletedFiles += 1;
      this.deletedRecords += file.recordCount();
    }

    public void addedFile(PartitionSpec spec, DataFile file) {
      updatePartitions(spec, file, true);
      this.addedFiles += 1;
      this.addedRecords += file.recordCount();
    }

    private void updatePartitions(PartitionSpec spec, DataFile file, boolean isAddition) {
      String key = spec.partitionToPath(file.partition());

      ScanSummary.PartitionMetrics metrics = changedPartitions.get(key);
      if (metrics == null) {
        metrics = new ScanSummary.PartitionMetrics();
      }

      if (isAddition) {
        // only add partition metrics for additions
        changedPartitions.put(key, metrics.updateFromFile(file, null));
      }
    }

    public Map<String, String> build() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      String genieId = new Configuration().get("genie.job.id");
      setIf(genieId != null, builder, GENIE_ID_PROP, genieId);
      setIf(addedFiles > 0, builder, ADDED_FILES_PROP, addedFiles);
      setIf(deletedFiles > 0, builder, DELETED_FILES_PROP, deletedFiles);
      setIf(deletedDuplicateFiles > 0, builder, DELETED_DUPLICATE_FILES, deletedDuplicateFiles);
      setIf(addedRecords > 0, builder, ADDED_RECORDS_PROP, addedRecords);
      setIf(deletedRecords > 0, builder, DELETED_RECORDS_PROP, deletedRecords);
      setIf(true, builder, CHANGED_PARTITION_COUNT_PROP, changedPartitions.size());

      if (changedPartitions.size() < 100) {
        setIf(true, builder, PARTITION_SUMMARY_PROP, "true");
        for (Map.Entry<String, ScanSummary.PartitionMetrics> entry : changedPartitions.entrySet()) {
          String key = entry.getKey();
          ScanSummary.PartitionMetrics metrics = entry.getValue();
          setIf(true, builder, CHANGED_PARTITION_PREFIX + key, MAP_JOINER.join(ImmutableMap.of(
              ADDED_FILES_PROP, metrics.fileCount(),
              ADDED_RECORDS_PROP, metrics.recordCount(),
              ADDED_FILE_SIZE_PROP, metrics.totalSize())));
        }
      }

      return builder.build();
    }

    private static void setIf(boolean expression, ImmutableMap.Builder<String, String> builder,
                              String property, Object value) {
      if (expression) {
        builder.put(property, String.valueOf(value));
      }
    }
  }
}
