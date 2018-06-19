package com.netflix.iceberg;

import com.netflix.iceberg.expressions.Expression;

import java.util.Set;

public interface OverwriteFiles extends PendingUpdate<Snapshot> {

  /**
   * Overwrites data for a given partition expression with the given set of data files.
   * If the partition expression contains any non partition columns or if evaluating partition
   * expression against any datafiles results in residual, the operation will be rejected.
   *
   * @param partition Expression to indicate which partition needs to be deleted.
   * @param files files that will replace the data files that match partition expression.
   * @return this instance for chaining.
   */
  OverwriteFiles overwrite(Expression partition, Set<DataFile> files);

}
