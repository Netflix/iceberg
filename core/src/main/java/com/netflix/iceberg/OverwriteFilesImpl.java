package com.netflix.iceberg;

import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Projections;

import java.util.Set;
import java.util.function.Predicate;

public class OverwriteFilesImpl extends BaseReplaceFiles implements OverwriteFiles {

  private final TableOperations ops;
  private Expression exp;

  OverwriteFilesImpl(TableOperations ops) {
    super(ops);
    this.ops = ops;
  }

  @Override
  public OverwriteFiles overwrite(Expression overwritePartitionExpression, Set<DataFile> files) {
    this.exp = overwritePartitionExpression;
    this.filesToAdd = files;
    this.hasNewFiles = true;
    return this;
  }

  @Override
  protected Predicate<ManifestEntry> shouldDelete(PartitionSpec spec) {
    return (ManifestEntry manifestEntry) -> {
      Expression strictExpr = Projections.strict(spec).project(exp);
      Evaluator strict = new Evaluator(spec.partitionType(), strictExpr);

      Expression inclusiveExpr = Projections.inclusive(spec).project(exp);
      Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
      if (inclusive.eval(manifestEntry.file().partition())) {
        if (strict.eval(manifestEntry.file().partition())) {
            return true;
        } else {
          throw new IllegalArgumentException("Can not overwrite for an expression that partially matches a datafile" + manifestEntry);
        }
      } else {
          return false;
      }
    };
  }
}
