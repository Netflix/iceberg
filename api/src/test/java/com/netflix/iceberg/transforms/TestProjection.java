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

package com.netflix.iceberg.transforms;

import com.google.common.collect.Lists;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.TestHelpers;
import com.netflix.iceberg.expressions.*;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.netflix.iceberg.TestHelpers.*;
import static com.netflix.iceberg.expressions.Expressions.and;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.greaterThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.lessThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.or;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

public class TestProjection {
  private static final Schema SCHEMA = new Schema(
    optional(16, "id", Types.LongType.get())
  );

  @Test
  public void testIdentityProjectionForUnary() {
    List<UnboundUnaryPredicate<Integer>> predicates = Lists.newArrayList(
      Expressions.notNull("id"),
      Expressions.isNull("id")
    );

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
      .identity("id")
      .build();

    for (UnboundUnaryPredicate<Integer> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.inclusive(spec).project(predicate);
      UnboundUnaryPredicate<Integer> projected = assertAndUnwrapUnboundUnary(expr);

      // check inclusive the boundUnary predicate to ensure the types are correct
      BoundUnaryPredicate<Integer> boundUnary = TestHelpers.assertAndUnwrapBoundUnary(predicate.bind(spec.schema().asStruct()));

      Assert.assertEquals("Field name should match partition struct field",
        "id", projected.ref().name());
      Assert.assertEquals("Operation should match", boundUnary.op(), projected.op());
      Assert.assertNull("Literal should be null", projected.literal());
    }
  }

  @Test
  public void testIdentityProjectionForValues() {
    List<UnboundValuePredicate<Integer>> predicates = Lists.newArrayList(
      Expressions.lessThan("id", 100),
      Expressions.lessThanOrEqual("id", 101),
      Expressions.greaterThan("id", 102),
      Expressions.greaterThanOrEqual("id", 103),
      Expressions.equal("id", 104),
      Expressions.notEqual("id", 105)
    );

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
      .identity("id")
      .build();

    for (UnboundValuePredicate<Integer> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.inclusive(spec).project(predicate);
      UnboundValuePredicate<Integer> projected = assertAndUnwrapUnboundValue(expr);

      // check inclusive the bound predicate to ensure the types are correct
      BoundValuePredicate<Integer> bound = TestHelpers.assertAndUnwrapBoundValue(predicate.bind(spec.schema().asStruct()));

      Assert.assertEquals("Field name should match partition struct field",
        "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      Assert.assertEquals("Literal should be equal",
        bound.literal().value(), projected.literal().value());
    }
  }

  @Test
  public void testIdentityProjectionForCollections() {
    List<UnboundCollectionPredicate<Integer>> predicates = Lists.newArrayList(
      Expressions.in("id", Lists.newArrayList(100, 101)),
      Expressions.notIn("id", Lists.newArrayList(100, 101))
    );

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
      .identity("id")
      .build();

    for (UnboundCollectionPredicate<Integer> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.inclusive(spec).project(predicate);
      UnboundCollectionPredicate<Integer> projected = assertAndUnwrapUnboundCollection(expr);

      // check inclusive the bound predicate to ensure the types are correct
      BoundCollectionPredicate<Integer> bound = TestHelpers.assertAndUnwrapBoundCollection(predicate.bind(spec.schema().asStruct()));

      Assert.assertEquals("Field name should match partition struct field",
        "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      Assert.assertArrayEquals("Literal should be equal",
        bound.literal().values().toArray(), projected.literal().values().toArray());
    }
  }

  @Test
  public void testStrictIdentityProjectionForUnary() {
    List<UnboundUnaryPredicate<Integer>> predicates = Lists.newArrayList(
      Expressions.notNull("id"),
      Expressions.isNull("id")
    );

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
      .identity("id")
      .build();

    for (UnboundUnaryPredicate<Integer> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.strict(spec).project(predicate);
      UnboundUnaryPredicate<Integer> projected = assertAndUnwrapUnboundUnary(expr);

      // check inclusive the boundUnary predicate to ensure the types are correct
      BoundUnaryPredicate<Integer> bound =
        TestHelpers.assertAndUnwrapBoundUnary(predicate.bind(spec.schema().asStruct()));

      Assert.assertEquals("Field name should match partition struct field",
        "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());
      Assert.assertNull("Literal should be null", projected.literal());
    }
  }

  @Test
  public void testStrictIdentityProjectionForValues() {
    List<UnboundValuePredicate<Integer>> predicates = Lists.newArrayList(
      Expressions.lessThan("id", 100),
      Expressions.lessThanOrEqual("id", 101),
      Expressions.greaterThan("id", 102),
      Expressions.greaterThanOrEqual("id", 103),
      Expressions.equal("id", 104),
      Expressions.notEqual("id", 105)
    );

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
      .identity("id")
      .build();

    for (UnboundValuePredicate<Integer> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.strict(spec).project(predicate);
      UnboundValuePredicate<Integer> projected = assertAndUnwrapUnboundValue(expr);

      // check inclusive the boundValue predicate to ensure the types are correct
      BoundValuePredicate<Integer> bound =
        TestHelpers.assertAndUnwrapBoundValue(predicate.bind(spec.schema().asStruct()));

      Assert.assertEquals("Field name should match partition struct field",
        "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      Assert.assertEquals("Literal should be equal",
        bound.literal().value(), projected.literal().value());
    }
  }

  @Test
  public void testStrictIdentityProjectionForCollections() {
    List<UnboundCollectionPredicate<Integer>> predicates = Lists.newArrayList(
      Expressions.in("id", Lists.newArrayList(100, 101)),
      Expressions.notIn("id", Lists.newArrayList(100, 101))
    );

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
      .identity("id")
      .build();

    for (UnboundCollectionPredicate<Integer> predicate : predicates) {
      // get the projected predicate
      Expression expr = Projections.strict(spec).project(predicate);
      UnboundCollectionPredicate<Integer> projected = assertAndUnwrapUnboundCollection(expr);

      // check inclusive the boundCollection predicate to ensure the types are correct
      BoundCollectionPredicate<Integer> bound =
        TestHelpers.assertAndUnwrapBoundCollection(predicate.bind(spec.schema().asStruct()));

      Assert.assertEquals("Field name should match partition struct field",
        "id", projected.ref().name());
      Assert.assertEquals("Operation should match", bound.op(), projected.op());

      Assert.assertArrayEquals("Literal should be equal",
        bound.literal().values().toArray(), projected.literal().values().toArray());
    }
  }

  @Test
  public void testBadSparkPartitionFilter() {
    // this tests a case that results in a full table scan in Spark with Hive tables. because the
    // hour field is not a partition, mixing it with partition columns in the filter expression
    // prevents the day/hour boundaries from being pushed to the metastore. this is an easy mistake
    // when tables are normally partitioned by both hour and dateint. the the filter is:
    //
    // WHERE dateint = 20180416
    //   OR (dateint = 20180415 and hour >= 20)
    //   OR (dateint = 20180417 and hour <= 4)

    Schema schema = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "hour", Types.IntegerType.get()),
      required(4, "dateint", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema)
      .identity("dateint")
      .build();

    Expression filter = or(equal("dateint", 20180416), or(
      and(equal("dateint", 20180415), greaterThanOrEqual("hour", 20)),
      and(equal("dateint", 20180417), lessThanOrEqual("hour", 4))));

    Expression projection = Projections.inclusive(spec).project(filter);

    Assert.assertTrue(projection instanceof Or);
    Or or1 = (Or) projection;
    UnboundValuePredicate<?> dateint1 = assertAndUnwrapUnboundValue(or1.left());
    Assert.assertEquals("Should be a dateint predicate", "dateint", dateint1.ref().name());
    Assert.assertEquals("Should be dateint=20180416", 20180416, dateint1.literal().value());
    Assert.assertTrue(or1.right() instanceof Or);
    Or or2 = (Or) or1.right();
    UnboundValuePredicate<?> dateint2 = assertAndUnwrapUnboundValue(or2.left());
    Assert.assertEquals("Should be a dateint predicate", "dateint", dateint2.ref().name());
    Assert.assertEquals("Should be dateint=20180415", 20180415, dateint2.literal().value());
    UnboundValuePredicate<?> dateint3 = assertAndUnwrapUnboundValue(or2.right());
    Assert.assertEquals("Should be a dateint predicate", "dateint", dateint3.ref().name());
    Assert.assertEquals("Should be dateint=20180417", 20180417, dateint3.literal().value());
  }
}
