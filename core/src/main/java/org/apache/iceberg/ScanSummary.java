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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

public class ScanSummary {
  private static final Set<String> IGNORED_OPERATIONS = Sets.newHashSet(
      DataOperations.DELETE, DataOperations.REPLACE);

  private ScanSummary() {
  }

  private static final List<String> SCAN_SUMMARY_COLUMNS = ImmutableList.of(
      "partition", "record_count", "file_size_in_bytes");

  /**
   * Create a scan summary builder for a table scan.
   *
   * @param scan a TableScan
   * @return a scan summary builder
   */
  public static ScanSummary.Builder of(TableScan scan) {
    return new Builder(scan);
  }

  public static class Builder {
    private static final Set<String> TIMESTAMP_NAMES = Sets.newHashSet(
        "dateCreated", "lastUpdated");
    private final TableScan scan;
    private final Table table;
    private final TableOperations ops;
    private final Map<Long, Long> snapshotTimestamps;
    private int limit = Integer.MAX_VALUE;
    private boolean throwIfLimited = false;
    private List<UnboundPredicate<Long>> timeFilters = Lists.newArrayList();
    private boolean forceUseManifests = false;

    public Builder(TableScan scan) {
      this.scan = scan;
      this.table = scan.table();
      this.ops = ((HasTableOperations) table).operations();
      ImmutableMap.Builder<Long, Long> builder = ImmutableMap.builder();
      for (Snapshot snap : table.snapshots()) {
        builder.put(snap.snapshotId(), snap.timestampMillis());
      }
      this.snapshotTimestamps = builder.build();
    }

    private void addTimestampFilter(UnboundPredicate<Long> filter) {
      throwIfLimited(); // ensure all partitions can be returned
      timeFilters.add(filter);
    }

    public Builder after(String timestamp) {
      Literal<Long> tsLiteral = Literal.of(timestamp).to(Types.TimestampType.withoutZone());
      return after(tsLiteral.value() / 1000);
    }

    public Builder after(long timestampMillis) {
      addTimestampFilter(Expressions.greaterThanOrEqual("timestamp_ms", timestampMillis));
      return this;
    }

    public Builder before(String timestamp) {
      Literal<Long> tsLiteral = Literal.of(timestamp).to(Types.TimestampType.withoutZone());
      return before(tsLiteral.value() / 1000);
    }

    public Builder before(long timestampMillis) {
      addTimestampFilter(Expressions.lessThanOrEqual("timestamp_ms", timestampMillis));
      return this;
    }

    public Builder throwIfLimited() {
      this.throwIfLimited = true;
      return this;
    }

    public Builder limit(int numPartitions) {
      this.limit = numPartitions;
      return this;
    }

    Builder useManifests() {
      // for testing, allow
      this.forceUseManifests = true;
      return this;
    }

    private void removeTimeFilters(List<Expression> expressions, Expression expression) {
      if (expression.op() == Expression.Operation.AND) {
        And and = (And) expression;
        removeTimeFilters(expressions, and.left());
        removeTimeFilters(expressions, and.right());
        return;

      } else if (expression instanceof UnboundPredicate) {
        UnboundPredicate pred = (UnboundPredicate) expression;
        NamedReference ref = (NamedReference) pred.ref();
        Literal<?> lit = pred.literal();
        if (TIMESTAMP_NAMES.contains(ref.name())) {
          Literal<Long> tsLiteral = lit.to(Types.TimestampType.withoutZone());
          long millis = toMillis(tsLiteral.value());
          addTimestampFilter(Expressions.predicate(pred.op(), "timestamp_ms", millis));
          return;
        }
      }

      expressions.add(expression);
    }

    /**
     * Summarizes a table scan as a map of partition key to metrics for that partition.
     *
     * @return a map from partition key to metrics for that partition.
     */
    public Map<String, PartitionMetrics> build() {
      if (table.currentSnapshot() == null) {
        return ImmutableMap.of(); // no snapshots, so there are no partitions
      }

      List<Expression> filters = Lists.newArrayList();
      removeTimeFilters(filters, Expressions.rewriteNot(scan.filter()));
      Expression rowFilter = joinFilters(filters);

      if (timeFilters.isEmpty()) {
        return fromManifestScan(table.currentSnapshot().manifests(), rowFilter);
      }

      Pair<Long, Long> range = timestampRange(timeFilters);
      long minTimestamp = range.first();
      long maxTimestamp = range.second();

      Snapshot oldestSnapshot = table.currentSnapshot();
      for (Map.Entry<Long, Long> entry : snapshotTimestamps.entrySet()) {
        if (entry.getValue() < oldestSnapshot.timestampMillis()) {
          oldestSnapshot = ops.current().snapshot(entry.getKey());
        }
      }

      // if the range starts before the oldest known snapshot, then there may be missing history. there could have been
      // a snapshot before the oldest and after the start that was expired. However, if the oldest known snapshot has
      // no parent, then it is the first snapshot in the table and table history can be trusted.
      if (oldestSnapshot.parentId() != null && minTimestamp < oldestSnapshot.timestampMillis()) {
        throw new IllegalArgumentException(
            "Cannot satisfy time filters: time range may include expired snapshots");
      }

      Iterable<Snapshot> snapshots = Iterables.filter(
          snapshotsInTimeRange(ops.current(), minTimestamp, maxTimestamp),
          snap -> !IGNORED_OPERATIONS.contains(snap.operation()));

      Map<String, PartitionMetrics> result = fromPartitionSummaries(snapshots);
      if (result != null && !forceUseManifests) {
        return result;
      }

      // filter down to the the set of manifest files that were created in the time range, ignoring
      // the snapshots created by delete or replace operations. this is complete because it finds
      // files in the snapshot where they were added to the dataset in either an append or an
      // overwrite. if those files are later compacted with a replace or deleted, those changes are
      // ignored.
      List<ManifestFile> manifestsToScan = Lists.newArrayList();
      Set<Long> snapshotIds = Sets.newHashSet();
      for (Snapshot snap : snapshots) {
        snapshotIds.add(snap.snapshotId());
        for (ManifestFile manifest : snap.manifests()) {
          // get all manifests added in the snapshot
          if (manifest.snapshotId() == null || manifest.snapshotId() == snap.snapshotId()) {
            manifestsToScan.add(manifest);
          }
        }
      }

      return fromManifestScan(manifestsToScan, rowFilter, true /* ignore existing entries */ );
    }

    private Map<String, PartitionMetrics> fromManifestScan(Iterable<ManifestFile> manifests,
                                                           Expression rowFilter) {
      return fromManifestScan(manifests, rowFilter, false /* all entries */ );
    }

    private Map<String, PartitionMetrics> fromManifestScan(
        Iterable<ManifestFile> manifests, Expression rowFilter, boolean ignoreExisting) {
      TopN<String, PartitionMetrics> topN = new TopN<>(
          limit, throwIfLimited, Comparators.charSequences());

      try (CloseableIterable<ManifestEntry> entries = new ManifestGroup(ops.io(), manifests)
          .specsById(ops.current().specsById())
          .filterData(rowFilter)
          .ignoreDeleted()
          .ignoreExisting(ignoreExisting)
          .select(SCAN_SUMMARY_COLUMNS)
          .entries()) {

        PartitionSpec spec = table.spec();
        for (ManifestEntry entry : entries) {
          Long timestamp = snapshotTimestamps.get(entry.snapshotId());
          String partition = spec.partitionToPath(entry.file().partition());
          topN.update(partition, metrics -> (metrics == null ? new PartitionMetrics() : metrics)
              .updateFromFile(entry.file(), timestamp));
        }

      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      return topN.get();
    }

    private Map<String, PartitionMetrics> fromPartitionSummaries(Iterable<Snapshot> snapshots) {
      // try to build the result from snapshot metadata, but fall back if:
      // * any snapshot has no summary
      // * any snapshot has
      TopN<String, PartitionMetrics> topN = new TopN<>(
          limit, throwIfLimited, Comparators.charSequences());

      for (Snapshot snap : snapshots) {
        if (snap.operation() == null || snap.summary() == null ||
            !Boolean.valueOf(snap.summary().getOrDefault(SnapshotSummary.PARTITION_SUMMARY_PROP, "false"))) {
          // the snapshot summary is missing or does not include partition-level data. fall back.
          return null;
        }

        for (Map.Entry<String, String> entry : snap.summary().entrySet()) {
          if (entry.getKey().startsWith(SnapshotSummary.CHANGED_PARTITION_PREFIX)) {
            String key = entry.getKey().substring(SnapshotSummary.CHANGED_PARTITION_PREFIX.length());
            Map<String, String> part = SnapshotSummary.MAP_SPLITTER.split(entry.getValue());
            int addedFiles = Integer.parseInt(part.getOrDefault(SnapshotSummary.ADDED_FILES_PROP, "0"));
            long addedRecords = Long.parseLong(part.getOrDefault(SnapshotSummary.ADDED_RECORDS_PROP, "0"));
            long addedSize = Long.parseLong(part.getOrDefault(SnapshotSummary.ADDED_FILE_SIZE_PROP, "0"));
            topN.update(key, metrics -> (metrics == null ? new PartitionMetrics() : metrics)
                .updateFromCounts(addedFiles, addedRecords, addedSize, snap.timestampMillis()));
          }
        }
      }

      return topN.get();
    }
  }

  private static Iterable<Snapshot> snapshotsInTimeRange(TableMetadata meta,
                                                         long minTimestamp, long maxTimestamp) {
    ImmutableList.Builder<Snapshot> snapshots = ImmutableList.builder();

    Snapshot current = meta.currentSnapshot();
    while (current != null && current.timestampMillis() >= minTimestamp) {
      if (current.timestampMillis() <= maxTimestamp) {
        snapshots.add(current);
      }

      if (current.parentId() != null) {
        current = meta.snapshot(current.parentId());
      } else {
        current = null;
      }
    }

    return snapshots.build().reverse();
  }

  public static class PartitionMetrics {
    private int fileCount = 0;
    private long recordCount = 0L;
    private long totalSize = 0L;
    private Long dataTimestampMillis = null;

    public int fileCount() {
      return fileCount;
    }

    public long recordCount() {
      return recordCount;
    }

    public long totalSize() {
      return totalSize;
    }

    public Long dataTimestampMillis() {
      return dataTimestampMillis;
    }

    PartitionMetrics updateFromCounts(int numFiles, long filesRecordCount, long filesSize,
                                      Long timestampMillis) {
      this.fileCount += numFiles;
      this.recordCount += filesRecordCount;
      this.totalSize += filesSize;
      if (timestampMillis != null && (dataTimestampMillis == null || dataTimestampMillis < timestampMillis)) {
        this.dataTimestampMillis = timestampMillis;
      }
      return this;
    }

    PartitionMetrics updateFromFile(DataFile file, Long timestampMillis) {
      this.fileCount += 1;
      this.recordCount += file.recordCount();
      this.totalSize += file.fileSizeInBytes();
      if (timestampMillis != null &&
          (dataTimestampMillis == null || dataTimestampMillis < timestampMillis)) {
        this.dataTimestampMillis = timestampMillis;
      }
      return this;
    }

    @Override
    public String toString() {
      String dataTimestamp = dataTimestampMillis != null ?
          new Date(dataTimestampMillis).toString() : null;
      return "PartitionMetrics(fileCount=" + fileCount +
          ", recordCount=" + recordCount +
          ", totalSize=" + totalSize +
          ", dataTimestamp=" + dataTimestamp + ")";
    }
  }

  private static class TopN<K, V> {
    private final int maxSize;
    private final boolean throwIfLimited;
    private final SortedMap<K, V> map;
    private final Comparator<? super K> keyComparator;
    private K cut = null;

    TopN(int maxSize, boolean throwIfLimited, Comparator<? super K> keyComparator) {
      this.maxSize = maxSize;
      this.throwIfLimited = throwIfLimited;
      this.map = Maps.newTreeMap(keyComparator);
      this.keyComparator = keyComparator;
    }

    public void update(K key, Function<V, V> updateFunc) {
      // if there is a cut and it comes before the given key, do nothing
      if (cut != null && keyComparator.compare(cut, key) <= 0) {
        return;
      }

      // call the update function and add the result to the map
      map.put(key, updateFunc.apply(map.get(key)));

      // enforce the size constraint and update the cut if some keys are excluded
      while (map.size() > maxSize) {
        if (throwIfLimited) {
          throw new IllegalStateException(
              String.format("Too many matching keys: more than %d", maxSize));
        }
        this.cut = map.lastKey();
        map.remove(cut);
      }
    }

    public Map<K, V> get() {
      return ImmutableMap.copyOf(map);
    }
  }

  static Expression joinFilters(List<Expression> expressions) {
    Expression result = Expressions.alwaysTrue();
    for (Expression expression : expressions) {
      result = Expressions.and(result, expression);
    }
    return result;
  }

  static long toMillis(long timestamp) {
    if (timestamp < 10000000000L) {
      // in seconds
      return timestamp * 1000;
    } else if (timestamp < 10000000000000L) {
      // in millis
      return timestamp;
    }
    // in micros
    return timestamp / 1000;
  }

  static Pair<Long, Long> timestampRange(List<UnboundPredicate<Long>> timeFilters) {
    // evaluation is inclusive
    long minTimestamp = Long.MIN_VALUE;
    long maxTimestamp = Long.MAX_VALUE;

    for (UnboundPredicate<Long> pred : timeFilters) {
      long value = pred.literal().value();
      switch (pred.op()) {
        case LT:
          if (value - 1 < maxTimestamp) {
            maxTimestamp = value - 1;
          }
          break;
        case LT_EQ:
          if (value < maxTimestamp) {
            maxTimestamp = value;
          }
          break;
        case GT:
          if (value + 1 > minTimestamp) {
            minTimestamp = value + 1;
          }
          break;
        case GT_EQ:
          if (value > minTimestamp) {
            minTimestamp = value;
          }
          break;
        case EQ:
          if (value < maxTimestamp) {
            maxTimestamp = value;
          }
          if (value > minTimestamp) {
            minTimestamp = value;
          }
          break;
        default:
          throw new UnsupportedOperationException(
              "Cannot filter timestamps using predicate: " + pred);
      }
    }

    if (maxTimestamp < minTimestamp) {
      throw new IllegalArgumentException(
          "No timestamps can match filters: " + Joiner.on(", ").join(timeFilters));
    }

    return Pair.of(minTimestamp, maxTimestamp);
  }
}
