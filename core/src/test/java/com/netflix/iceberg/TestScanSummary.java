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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import java.util.Map;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.netflix.iceberg.ScanSummary.timestampRange;
import static com.netflix.iceberg.ScanSummary.toMillis;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.greaterThan;
import static com.netflix.iceberg.expressions.Expressions.greaterThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.lessThan;
import static com.netflix.iceberg.expressions.Expressions.lessThanOrEqual;

public class TestScanSummary extends TableTestBase {

  @Test
  public void testSnapshotTimeRangeValidation() {
    long t0 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t1 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long secondSnapshotId = table.currentSnapshot().snapshotId();

    long t2 = System.currentTimeMillis();

    // expire the first snapshot
    table.expireSnapshots()
        .expireOlderThan(t1)
        .commit();

    Assert.assertEquals("Should have one snapshot",
        1, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals("Snapshot should be the second snapshot created",
        secondSnapshotId, table.currentSnapshot().snapshotId());

    // this should include the first snapshot, but it was removed from the dataset
    TableScan scan = table.newScan()
        .filter(greaterThanOrEqual("dateCreated", t0))
        .filter(lessThan("dateCreated", t2));

    AssertHelpers.assertThrows("Should fail summary because range may include expired snapshots",
        IllegalArgumentException.class, "may include expired snapshots",
        () -> new ScanSummary.Builder(scan).useManifests().build());

    AssertHelpers.assertThrows("Should fail summary because range may include expired snapshots",
        IllegalArgumentException.class, "may include expired snapshots",
        () -> new ScanSummary.Builder(scan).build());
  }

  @Test
  public void testManifestScanSummary() {
    // create an initial snapshot
    table.newAppend().commit();

    long t0 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long timestamp = table.currentSnapshot().timestampMillis();

    long t1 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    TableScan scan = table.newScan()
        .filter(greaterThanOrEqual("dateCreated", t0))
        .filter(lessThan("dateCreated", t1));

    Map<String, ScanSummary.PartitionMetrics> partitions = new ScanSummary.Builder(scan)
        .useManifests()
        .build();

    Assert.assertEquals("Should produce a summary of the changed partitions",
        Sets.newHashSet("data_bucket=0", "data_bucket=1"), partitions.keySet());
    Assert.assertEquals(1, partitions.get("data_bucket=0").fileCount());
    Assert.assertEquals(0, partitions.get("data_bucket=0").recordCount());
    Assert.assertEquals(0, partitions.get("data_bucket=0").totalSize());
    Assert.assertEquals((Long) timestamp, partitions.get("data_bucket=0").dataTimestampMillis());
  }

  @Test
  public void testScanSummarySkipsMergeAndDelete() {
    // create an initial snapshot
    table.newAppend().commit();

    long t0 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    table.newRewrite()
        .rewriteFiles(Sets.newHashSet(FILE_A, FILE_B), Sets.newHashSet(FILE_D))
        .commit();

    table.newDelete()
        .deleteFile(FILE_D.path())
        .commit();

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t1 = System.currentTimeMillis();

    TableScan scan = table.newScan()
        .filter(greaterThanOrEqual("dateCreated", t0))
        .filter(lessThan("dateCreated", t1));

    Map<String, ScanSummary.PartitionMetrics> partitions = new ScanSummary.Builder(scan)
        .useManifests()
        .build();

    Assert.assertEquals("Should produce a summary of the changed partitions",
        Sets.newHashSet("data_bucket=0", "data_bucket=1", "data_bucket=2"), partitions.keySet());
  }

  @Test
  public void testScanSummaryHandlesManifestCompaction() {
    // create an initial snapshot
    table.newAppend().commit();

    // trigger manifest compaction on every commit
    table.updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    long t0 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    table.newRewrite()
        .rewriteFiles(Sets.newHashSet(FILE_A, FILE_B), Sets.newHashSet(FILE_D))
        .commit();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    table.newDelete()
        .deleteFile(FILE_D.path())
        .commit();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    long t1 = System.currentTimeMillis();

    TableScan scan = table.newScan()
        .filter(greaterThanOrEqual("dateCreated", t0))
        .filter(lessThan("dateCreated", t1));

    Map<String, ScanSummary.PartitionMetrics> partitions = new ScanSummary.Builder(scan)
        .useManifests()
        .build();

    Assert.assertEquals("Should produce a summary of the changed partitions",
        Sets.newHashSet("data_bucket=0", "data_bucket=1", "data_bucket=2"), partitions.keySet());
  }

  @Test
  public void testScanSummaryCountsAfterManifestCompaction() {
    // create an initial snapshot
    table.newAppend().commit();

    // trigger manifest compaction on every commit
    table.updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    long t0 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    long t1 = System.currentTimeMillis();

    TableScan scan = table.newScan()
        .filter(greaterThanOrEqual("dateCreated", t0))
        .filter(lessThan("dateCreated", t1));

    // to produce the summary, the builder will scan the manifest where A and B were added and the
    // manifest where C was added. because compaction is turned on, the manifest where C was added
    // contains A and B as existing entries. the files should be ignored as existing and should not
    // be counted twice.
    Map<String, ScanSummary.PartitionMetrics> partitions = new ScanSummary.Builder(scan)
        .useManifests()
        .build();

    Assert.assertEquals("Should produce a summary of the changed partitions",
        Sets.newHashSet("data_bucket=0", "data_bucket=1", "data_bucket=2"), partitions.keySet());
    Assert.assertEquals(1, partitions.get("data_bucket=0").fileCount());
  }

  @Test
  public void testMetadataOnlyScanSummary() {
    // create an initial snapshot
    table.newAppend().commit();

    long t0 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t1 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    // delete the manifest files to ensure they are not read
    FileIO io = table.ops().io();
    Sets.newHashSet(concat(transform(table.snapshots(), snap -> snap.manifests())))
        .forEach(manifest -> io.deleteFile(manifest.path()));

    TableScan scan = table.newScan()
        .filter(greaterThanOrEqual("dateCreated", t0))
        .filter(lessThan("dateCreated", t1));

    Map<String, ScanSummary.PartitionMetrics> partitions = new ScanSummary.Builder(scan).build();

    Assert.assertEquals("Should produce a summary of the changed partitions",
        Sets.newHashSet("data_bucket=0", "data_bucket=1"), partitions.keySet());
    Assert.assertEquals(1, partitions.get("data_bucket=0").fileCount());
    Assert.assertEquals(0, partitions.get("data_bucket=0").recordCount());
    Assert.assertEquals(0, partitions.get("data_bucket=0").totalSize());
  }

  @Test
  public void testMetadataOnlyScanSummarySkipsMergeAndDelete() {
    // create an initial snapshot
    table.newAppend().commit();

    long t0 = System.currentTimeMillis();

    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    table.newRewrite()
        .rewriteFiles(Sets.newHashSet(FILE_A, FILE_B), Sets.newHashSet(FILE_D))
        .commit();

    table.newDelete()
        .deleteFile(FILE_D.path())
        .commit();

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t1 = System.currentTimeMillis();

    // delete the manifest files to ensure they are not read
    FileIO io = table.ops().io();
    Sets.newHashSet(concat(transform(table.snapshots(), snap -> snap.manifests())))
        .forEach(manifest -> io.deleteFile(manifest.path()));

    TableScan scan = table.newScan()
        .filter(greaterThanOrEqual("dateCreated", t0))
        .filter(lessThan("dateCreated", t1));

    Map<String, ScanSummary.PartitionMetrics> partitions = new ScanSummary.Builder(scan).build();

    Assert.assertEquals("Should produce a summary of the changed partitions",
        Sets.newHashSet("data_bucket=0", "data_bucket=1", "data_bucket=2"), partitions.keySet());
  }

  @Test
  public void testTimestampRanges() {
    long lower = 1542750188523L;
    long upper = 1542750695131L;

    Assert.assertEquals("Should use inclusive bound",
        Pair.of(Long.MIN_VALUE, upper),
        timestampRange(ImmutableList.of(lessThanOrEqual("ts_ms", upper))));

    Assert.assertEquals("Should use lower value for upper bound",
        Pair.of(Long.MIN_VALUE, upper),
        timestampRange(ImmutableList.of(
            lessThanOrEqual("ts_ms", upper + 918234),
            lessThanOrEqual("ts_ms", upper))));

    Assert.assertEquals("Should make upper bound inclusive",
        Pair.of(Long.MIN_VALUE, upper - 1),
        timestampRange(ImmutableList.of(lessThan("ts_ms", upper))));

    Assert.assertEquals("Should use inclusive bound",
        Pair.of(lower, Long.MAX_VALUE),
        timestampRange(ImmutableList.of(greaterThanOrEqual("ts_ms", lower))));

    Assert.assertEquals("Should use upper value for lower bound",
        Pair.of(lower, Long.MAX_VALUE),
        timestampRange(ImmutableList.of(
            greaterThanOrEqual("ts_ms", lower - 918234),
            greaterThanOrEqual("ts_ms", lower))));

    Assert.assertEquals("Should make lower bound inclusive",
        Pair.of(lower + 1, Long.MAX_VALUE),
        timestampRange(ImmutableList.of(greaterThan("ts_ms", lower))));

    Assert.assertEquals("Should set both bounds for equals",
        Pair.of(lower, lower),
        timestampRange(ImmutableList.of(equal("ts_ms", lower))));

    Assert.assertEquals("Should set both bounds",
        Pair.of(lower, upper - 1),
        timestampRange(ImmutableList.of(
            greaterThanOrEqual("ts_ms", lower),
            lessThan("ts_ms", upper))));

    // >= lower and < lower is an empty range
    AssertHelpers.assertThrows("Should reject empty ranges",
        IllegalArgumentException.class, "No timestamps can match filters",
        () -> timestampRange(ImmutableList.of(
            greaterThanOrEqual("ts_ms", lower),
            lessThan("ts_ms", lower))));
  }

  @Test
  public void testToMillis() {
    long millis = 1542750947417L;
    Assert.assertEquals(1542750947000L, toMillis(millis / 1000));
    Assert.assertEquals(1542750947417L, toMillis(millis));
    Assert.assertEquals(1542750947417L, toMillis(millis * 1000 + 918));
  }
}
