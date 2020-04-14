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

package com.netflix.iceberg.metacat;

import org.junit.Assert;
import org.junit.Test;

public class TestTableRef {
  @Test
  public void testSimpleTableNameParsing() {
    TableRef ref = TableRef.parse("table");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testHistoryTableNameParsing() {
    TableRef ref = TableRef.parse("table$history");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.HISTORY, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testSnapshotsTableNameParsing() {
    TableRef ref = TableRef.parse("table$snapshots");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.SNAPSHOTS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testPartitionsTableNameParsing() {
    TableRef ref = TableRef.parse("table$partitions");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.PARTITIONS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testManifestsTableNameParsing() {
    TableRef ref = TableRef.parse("table$manifests");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.MANIFESTS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAt() {
    TableRef ref = TableRef.parse("table@1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtAt() {
    // cannot parse 1234@4567 as a long
    TableRef ref = TableRef.parse("table@1234@4567");
    Assert.assertEquals("table@1234@4567", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtText() {
    // cannot parse history as a long
    TableRef ref = TableRef.parse("table@history");
    Assert.assertEquals("table@history", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testHistoryAt() {
    // history is not compatible with at
    TableRef ref = TableRef.parse("table$history@1234");
    Assert.assertEquals("table$history@1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtHistory() {
    // history is not compatible with at
    TableRef ref = TableRef.parse("table@1234$history");
    Assert.assertEquals("table@1234$history", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testSnapshotsAt() {
    // snapshots is not compatible with at
    TableRef ref = TableRef.parse("table$snapshots@1234");
    Assert.assertEquals("table$snapshots@1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtSnapshots() {
    // snapshots is not compatible with at
    TableRef ref = TableRef.parse("table@1234$snapshots");
    Assert.assertEquals("table@1234$snapshots", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testPartitionsAt() {
    TableRef ref = TableRef.parse("table$partitions@1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.PARTITIONS, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtPartitions() {
    TableRef ref = TableRef.parse("table@1234$partitions");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.PARTITIONS, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testManifestsAt() {
    TableRef ref = TableRef.parse("table$manifests@1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.MANIFESTS, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtManifests() {
    TableRef ref = TableRef.parse("table@1234$manifests");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.MANIFESTS, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testBadTableType() {
    // not a valid name
    TableRef ref = TableRef.parse("table$parts");
    Assert.assertEquals("table$parts", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testHistoryTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__history");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.HISTORY, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testSnapshotsTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__snapshots");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.SNAPSHOTS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testPartitionsTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__partitions");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.PARTITIONS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testManifestsTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__manifests");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.MANIFESTS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtWithUnderscores() {
    TableRef ref = TableRef.parse("table__1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtAtWithUnderscores() {
    // cannot parse 1234 as a table name
    TableRef ref = TableRef.parse("table__1234__4567");
    Assert.assertEquals("table__1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertEquals(4567L, (long) ref.at());
  }

  @Test
  public void testHistoryAtWithUnderscores() {
    // history is not compatible with at
    TableRef ref = TableRef.parse("table__history__1234");
    Assert.assertEquals("table__history__1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtHistoryWithUnderscores() {
    // history is not compatible with at
    TableRef ref = TableRef.parse("table__1234__history");
    Assert.assertEquals("table__1234__history", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testSnapshotsAtWithUnderscores() {
    // snapshots is not compatible with at
    TableRef ref = TableRef.parse("table__snapshots__1234");
    Assert.assertEquals("table__snapshots__1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtSnapshotsWithUnderscores() {
    // snapshots is not compatible with at
    TableRef ref = TableRef.parse("table__1234__snapshots");
    Assert.assertEquals("table__1234__snapshots", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testPartitionsAtWithUnderscores() {
    TableRef ref = TableRef.parse("table__partitions__1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.PARTITIONS, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtPartitionsWithUnderscores() {
    TableRef ref = TableRef.parse("table__1234__partitions");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.PARTITIONS, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testManifestsAtWithUnderscores() {
    TableRef ref = TableRef.parse("table__manifests__1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.MANIFESTS, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtManifestsWithUnderscores() {
    TableRef ref = TableRef.parse("table__1234__manifests");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.MANIFESTS, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testBadTableTypeWithUnderscores() {
    // not a valid name
    TableRef ref = TableRef.parse("table__parts");
    Assert.assertEquals("table__parts", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testTableWithTwoSnapshots() {
    String name = "cass_subscriberservice_1__subscriber_data__account_profiles__snapshots$snapshots";
    TableRef ref = TableRef.parse(name);
    Assert.assertEquals("cass_subscriberservice_1__subscriber_data__account_profiles__snapshots", ref.table());
    Assert.assertEquals(TableType.SNAPSHOTS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testEntriesTableNameParsing() {
    TableRef ref = TableRef.parse("table$entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testEntriesAt() {
    TableRef ref = TableRef.parse("table$entries@1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtEntries() {
    TableRef ref = TableRef.parse("table@1234$entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testEntriesTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testEntriesAtWithUnderscores() {
    TableRef ref = TableRef.parse("table__entries__1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtEntriesWithUnderscores() {
    TableRef ref = TableRef.parse("table__1234__entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testFilesTableNameParsing() {
    TableRef ref = TableRef.parse("table$entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testFilesAt() {
    TableRef ref = TableRef.parse("table$entries@1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtFiles() {
    TableRef ref = TableRef.parse("table@1234$entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testFilesTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testFilesAtWithUnderscores() {
    TableRef ref = TableRef.parse("table__entries__1234");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAtFilesWithUnderscores() {
    TableRef ref = TableRef.parse("table__1234__entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ENTRIES, ref.type());
    Assert.assertEquals(1234L, (long) ref.at());
  }

  @Test
  public void testAllDataFilesTableNameParsing() {
    TableRef ref = TableRef.parse("table$all_data_files");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ALL_DATA_FILES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllDataFilesAt() {
    // all_data_files is not compatible with at
    TableRef ref = TableRef.parse("table$all_data_files@1234");
    Assert.assertEquals("table$all_data_files@1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtAllDataFiles() {
    // all_data_files is not compatible with at
    TableRef ref = TableRef.parse("table@1234$all_data_files");
    Assert.assertEquals("table@1234$all_data_files", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllDataFilesTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__all_data_files");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ALL_DATA_FILES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllDataFilesAtWithUnderscores() {
    // all_data_files is not compatible with at
    TableRef ref = TableRef.parse("table__all_data_files__1234");
    Assert.assertEquals("table__all_data_files__1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtAllDataFilesWithUnderscores() {
    // all_data_files is not compatible with at
    TableRef ref = TableRef.parse("table__1234__all_data_files");
    Assert.assertEquals("table__1234__all_data_files", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testTableWithTwoAllDataFiles() {
    String name = "cass_subscriberservice_1__subscriber_data__account_profiles__all_data_files$all_data_files";
    TableRef ref = TableRef.parse(name);
    Assert.assertEquals("cass_subscriberservice_1__subscriber_data__account_profiles__all_data_files", ref.table());
    Assert.assertEquals(TableType.ALL_DATA_FILES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllManifestsTableNameParsing() {
    TableRef ref = TableRef.parse("table$all_manifests");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ALL_MANIFESTS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllManifestsAt() {
    // all_manifests is not compatible with at
    TableRef ref = TableRef.parse("table$all_manifests@1234");
    Assert.assertEquals("table$all_manifests@1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtAllManifests() {
    // all_manifests is not compatible with at
    TableRef ref = TableRef.parse("table@1234$all_manifests");
    Assert.assertEquals("table@1234$all_manifests", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllManifestsTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__all_manifests");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ALL_MANIFESTS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllManifestsAtWithUnderscores() {
    // all_manifests is not compatible with at
    TableRef ref = TableRef.parse("table__all_manifests__1234");
    Assert.assertEquals("table__all_manifests__1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtAllManifestsWithUnderscores() {
    // all_manifests is not compatible with at
    TableRef ref = TableRef.parse("table__1234__all_manifests");
    Assert.assertEquals("table__1234__all_manifests", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testTableWithTwoAllManifests() {
    String name = "cass_subscriberservice_1__subscriber_data__account_profiles__all_manifests$all_manifests";
    TableRef ref = TableRef.parse(name);
    Assert.assertEquals("cass_subscriberservice_1__subscriber_data__account_profiles__all_manifests", ref.table());
    Assert.assertEquals(TableType.ALL_MANIFESTS, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllEntriesTableNameParsing() {
    TableRef ref = TableRef.parse("table$all_entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ALL_ENTRIES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllEntriesAt() {
    // all_entries is not compatible with at
    TableRef ref = TableRef.parse("table$all_entries@1234");
    Assert.assertEquals("table$all_entries@1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtAllEntries() {
    // all_entries is not compatible with at
    TableRef ref = TableRef.parse("table@1234$all_entries");
    Assert.assertEquals("table@1234$all_entries", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllEntriesTableNameParsingWithUnderscores() {
    TableRef ref = TableRef.parse("table__all_entries");
    Assert.assertEquals("table", ref.table());
    Assert.assertEquals(TableType.ALL_ENTRIES, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAllEntriesAtWithUnderscores() {
    // all_entries is not compatible with at
    TableRef ref = TableRef.parse("table__all_entries__1234");
    Assert.assertEquals("table__all_entries__1234", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testAtAllEntriesWithUnderscores() {
    // all_entries is not compatible with at
    TableRef ref = TableRef.parse("table__1234__all_entries");
    Assert.assertEquals("table__1234__all_entries", ref.table());
    Assert.assertEquals(TableType.DATA, ref.type());
    Assert.assertNull(ref.at());
  }

  @Test
  public void testTableWithTwoAllEntries() {
    String name = "cass_subscriberservice_1__subscriber_data__account_profiles__all_entries$all_entries";
    TableRef ref = TableRef.parse(name);
    Assert.assertEquals("cass_subscriberservice_1__subscriber_data__account_profiles__all_entries", ref.table());
    Assert.assertEquals(TableType.ALL_ENTRIES, ref.type());
    Assert.assertNull(ref.at());
  }
}
