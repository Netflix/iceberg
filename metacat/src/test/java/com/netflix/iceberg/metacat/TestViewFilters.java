package com.netflix.iceberg.metacat;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestViewFilters {
  @Test
  public void testPartitionsToFilter() {
    PartitionSpec spec = PartitionSpec.builderFor(
        new Schema(
            required(1, "dateint", Types.IntegerType.get()),
            required(2, "batchid", Types.IntegerType.get())))
        .identity("dateint")
        .identity("batchid")
        .build();

    List<String> partitionKeys = ImmutableList.of(
        "dateint=20190103/batchid=2",
        "dateint=20190102/batchid=2");

    Expression expected = Expressions.or(
        Expressions.and(Expressions.equal("dateint", 20190103), Expressions.equal("batchid", 2)),
        Expressions.and(Expressions.equal("dateint", 20190102), Expressions.equal("batchid", 2)));

    Assert.assertEquals("Expressions (as strings) should match",
        expected.toString(),
        PigMetacatTables.partitionFilter(spec, partitionKeys).toString());
  }
}
