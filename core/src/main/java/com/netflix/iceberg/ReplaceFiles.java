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

package com.netflix.iceberg;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.util.Tasks;
import com.netflix.iceberg.util.ThreadPools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.synchronizedList;

class ReplaceFiles extends SnapshotUpdate implements RewriteFiles {

  private final TableOperations ops;
  private final Set<DataFile> filesToAdd = new HashSet<>();
  private final Set<CharSequence> pathsToDelete = new HashSet<>();
  private final List<String> newManifests = synchronizedList(new ArrayList<>());
  private List<String> manifestsToCommit = synchronizedList(new ArrayList<>());
  private List<String> appendManifests = new ArrayList<>();
  private final AtomicInteger manifestCount = new AtomicInteger(0);
  private boolean hasNewFiles;

  ReplaceFiles(TableOperations ops) {
    super(ops);
    this.ops = ops;
  }

  /**
   * @param base TableMetadata of the base snapshot
   * @return list of manifestsToCommit that "may" get committed if commit is called on this instance.
   */
  @Override
  protected List<String> apply(TableMetadata base) {
    Preconditions.checkArgument(!(this.pathsToDelete.isEmpty() || this.filesToAdd.isEmpty()),
            "Must provide some files to add and delete.");

    this.manifestsToCommit.clear(); //clear the state.

    final Snapshot snapshot = base.currentSnapshot();
      ValidationException.check(snapshot != null, "No snapshots are committed.");

      final List<String> currentManifests = snapshot.manifests();
      final List<CharSequence> deletedPaths = synchronizedList(new ArrayList<>());

      Tasks.foreach(currentManifests)
        .noRetry()
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(manifest -> {
          final OutputFile manifestPath = manifestPath(manifestCount.getAndIncrement());
          try (ManifestReader manifestReader = ManifestReader.read(ops.newInputFile(manifest))) {
            int numDeletedFiles = deletedPaths.size();
            try (ManifestWriter writer = new ManifestWriter(manifestReader.spec(), manifestPath, snapshotId())) {
              manifestReader.notDeletedEntries()
                .forEach(manifestEntry -> {
                  if (this.pathsToDelete.contains(manifestEntry.file().path())) {
                    writer.delete(manifestEntry);
                    deletedPaths.add(manifestEntry.file().path());
                  } else {
                    writer.addExisting(manifestEntry);
                  }
                });
            }

            // If no files were deleted as processing of this manifest, Delete the newly created
            // manifest and keep the existing manifest in the new snapshot as is.
            if (numDeletedFiles == deletedPaths.size()) {
              deleteFile(manifestPath.location());
              this.manifestsToCommit.add(manifest);
            } else {
              this.manifestsToCommit.add(manifestPath.location());
              this.newManifests.add(manifestPath.location());
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Could not read manifestFile ", manifest);
          }
        });

      if (deletedPaths.size() != pathsToDelete.size()) {
        final String paths = pathsToDelete.stream()
                .filter(path -> !deletedPaths.contains(path))
                .collect(Collectors.joining(","));
        String msg = format("files %s are no longer available in any manifestsToCommit", paths);
        throw new CommitFailedException(msg);
      }

    addFiles(base.spec());

    return this.manifestsToCommit;
  }

  private void addFiles(PartitionSpec spec) {
    if (this.hasNewFiles) {
      OutputFile out = manifestPath(manifestCount.getAndIncrement());
      try (ManifestWriter writer = new ManifestWriter(spec, out, snapshotId())) {
        writer.addAll(this.filesToAdd);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to write manifest: %s", out);
      }
      appendManifests.add(out.location());
      newManifests.add(out.location());
    }
    this.hasNewFiles = false;
    manifestsToCommit.addAll(appendManifests);
  }

  @Override
  protected void cleanUncommitted(Set<String> committed) {
    newManifests.stream().filter(m -> !committed.contains(m)).forEach(m -> deleteFile(m));
    newManifests.clear();
  }

  @Override
  public RewriteFiles rewriteFiles(Set<DataFile> filesToDelete, Set<DataFile> filesToAdd) {
    Preconditions.checkArgument(filesToDelete != null && !filesToDelete.isEmpty(), "files to delete can not be null or empty");
    Preconditions.checkArgument(filesToAdd != null && !filesToAdd.isEmpty(), "files to add can not be null or empty");

    this.pathsToDelete.addAll(filesToDelete.stream().map(d -> d.path()).collect(Collectors.toList()));
    this.filesToAdd.addAll(filesToAdd);
    this.hasNewFiles = true;
    return this;
  }
}
