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

package org.apache.iceberg;

import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

public class SnapshotSummary {
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
  public static final String STAGED_WAP_ID_PROP = "wap.id";
  public static final String PUBLISHED_WAP_ID_PROP = "published-wap-id";
  public static final String SOURCE_SNAPSHOT_ID_PROP = "source-snapshot-id";

  private SnapshotSummary() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    // commit summary tracking
    private Map<String, ScanSummary.PartitionMetrics> addedByPartition = Maps.newHashMap();
    private Map<String, ScanSummary.PartitionMetrics> deletedByPartition = Maps.newHashMap();
    private long addedFiles = 0L;
    private long deletedFiles = 0L;
    private long deletedDuplicateFiles = 0L;
    private long addedRecords = 0L;
    private long deletedRecords = 0L;
    private Map<String, String> properties = Maps.newHashMap();

    public void clear() {
      addedByPartition.clear();
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

    public void set(String property, String value) {
      properties.put(property, value);
    }

    private Set<String> changedPartitionKeys() {
      Set<String> changedPartitionKeys = Sets.newHashSet();
      changedPartitionKeys.addAll(addedByPartition.keySet());
      changedPartitionKeys.addAll(deletedByPartition.keySet());
      return changedPartitionKeys;
    }

    private void updatePartitions(PartitionSpec spec, DataFile file, boolean isAddition) {
      String key = spec.partitionToPath(file.partition());

      if (isAddition) {
        updatePartitionMetrics(addedByPartition, key, file);
      } else {
        updatePartitionMetrics(deletedByPartition, key, file);
      }
    }

    private void updatePartitionMetrics(Map<String, ScanSummary.PartitionMetrics> metricsMap,
                                        String key, DataFile file) {
      ScanSummary.PartitionMetrics metrics = metricsMap.get(key);
      if (metrics == null) {
        metrics = new ScanSummary.PartitionMetrics();
      }

      // only add partition metrics for additions
      metricsMap.put(key, metrics.updateFromFile(file, null));
    }

    public void merge(SnapshotSummary.Builder builder) {
      for (Map.Entry<String, ScanSummary.PartitionMetrics> entry : builder.addedByPartition.entrySet()) {
        ScanSummary.PartitionMetrics metrics = addedByPartition.get(entry.getKey());
        if (metrics == null) {
          metrics = new ScanSummary.PartitionMetrics();
          addedByPartition.put(entry.getKey(), metrics);
        }

        ScanSummary.PartitionMetrics newMetrics = entry.getValue();

        metrics.updateFromCounts(
            newMetrics.fileCount(), newMetrics.recordCount(), newMetrics.totalSize(), newMetrics.dataTimestampMillis());
      }
      for (Map.Entry<String, ScanSummary.PartitionMetrics> entry : builder.deletedByPartition.entrySet()) {
        ScanSummary.PartitionMetrics metrics = deletedByPartition.get(entry.getKey());
        if (metrics == null) {
          metrics = new ScanSummary.PartitionMetrics();
          deletedByPartition.put(entry.getKey(), metrics);
        }

        ScanSummary.PartitionMetrics newMetrics = entry.getValue();

        metrics.updateFromCounts(
            newMetrics.fileCount(), newMetrics.recordCount(), newMetrics.totalSize(), newMetrics.dataTimestampMillis());
      }
      this.addedFiles += builder.addedFiles;
      this.deletedFiles += builder.deletedFiles;
      this.deletedDuplicateFiles += builder.deletedDuplicateFiles;
      this.addedRecords += builder.addedRecords;
      this.deletedRecords += builder.deletedRecords;
      this.properties.putAll(builder.properties);
    }

    public Map<String, String> build() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      // copy custom summary properties
      builder.putAll(properties);

      setIf(addedFiles > 0, builder, ADDED_FILES_PROP, addedFiles);
      setIf(deletedFiles > 0, builder, DELETED_FILES_PROP, deletedFiles);
      setIf(deletedDuplicateFiles > 0, builder, DELETED_DUPLICATE_FILES, deletedDuplicateFiles);
      setIf(addedRecords > 0, builder, ADDED_RECORDS_PROP, addedRecords);
      setIf(deletedRecords > 0, builder, DELETED_RECORDS_PROP, deletedRecords);

      Set<String> changedPartitions = changedPartitionKeys();
      setIf(true, builder, CHANGED_PARTITION_COUNT_PROP, changedPartitions.size());

      if (changedPartitions.size() < 100) {
        setIf(true, builder, PARTITION_SUMMARY_PROP, "true");
        for (String key : changedPartitions) {
          setIf(key != null, builder, CHANGED_PARTITION_PREFIX + key, partitionSummary(key));
        }
      }

      return builder.build();
    }

    private String partitionSummary(String key) {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      ScanSummary.PartitionMetrics addMetrics = addedByPartition.get(key);
      ScanSummary.PartitionMetrics deleteMetrics = deletedByPartition.get(key);

      int addedPartFiles = addMetrics != null ? addMetrics.fileCount() : 0;
      long addedPartRecords = addMetrics != null ? addMetrics.recordCount() : 0;
      long addedPartSize = addMetrics != null ? addMetrics.totalSize() : 0;
      int deletedPartFiles = deleteMetrics != null ? deleteMetrics.fileCount() : 0;
      long deletedPartRecords = deleteMetrics != null ? deleteMetrics.recordCount() : 0;

      setIf(addedPartFiles > 0, builder, ADDED_FILES_PROP, addedPartFiles);
      setIf(addedPartRecords > 0, builder, ADDED_RECORDS_PROP, addedPartRecords);
      setIf(addedPartSize > 0, builder, ADDED_FILE_SIZE_PROP, addedPartSize);
      setIf(deletedPartFiles > 0, builder, DELETED_FILES_PROP, deletedPartFiles);
      setIf(deletedPartRecords > 0, builder, DELETED_RECORDS_PROP, deletedPartRecords);

      return MAP_JOINER.join(builder.build());
    }

    private static void setIf(boolean expression, ImmutableMap.Builder<String, String> builder,
                              String property, Object value) {
      if (expression) {
        builder.put(property, String.valueOf(value));
      }
    }
  }
}
