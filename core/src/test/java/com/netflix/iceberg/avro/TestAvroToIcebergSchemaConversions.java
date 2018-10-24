package com.netflix.iceberg.avro;

import com.netflix.iceberg.types.Types;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestAvroToIcebergSchemaConversions {
  @Test
  public void testRecordWithRequiredAndOptionalConversion() {
    Schema schema = SchemaBuilder.record("foo")
        .fields()
        .requiredString("bar")
        .optionalBoolean("baz")
        .endRecord();

    com.netflix.iceberg.Schema result =
        AvroToIcebergSchemaVisitor.visit(schema, schema.getName(), new AvroToIcebergSchemaVisitor(schema));

    Types.StructType record = result.asStruct();

    Types.NestedField barField = record.field("bar");
    Assert.assertEquals(barField.fieldId(), 0);
    Assert.assertEquals(barField.name(), "bar");
    Assert.assertTrue(barField.isRequired());

    Types.NestedField bazField = record.field("baz");
    Assert.assertEquals(bazField.fieldId(), 1);
    Assert.assertEquals(bazField.name(), "baz");
    Assert.assertTrue(bazField.isOptional());
  }

  @Test
  public void testNestedRecordsWithRequiredAndOptionalConversion() {
    Schema schema = SchemaBuilder.record("foo")
        .fields()
        .name("bar")
        .type()
        .record("baz")
        .fields()
        .optionalDouble("number")
        .requiredLong("otherNumber")
        .endRecord()
        .noDefault()
        .optionalBytes("myBytes")
        .endRecord();

    Schema endUnion = SchemaBuilder.array().items().unionOf().booleanBuilder().endBoolean().endUnion();

    com.netflix.iceberg.Schema icebergSchema = AvroNamedSchemaVisitor.visit(schema, schema.getName(), new AvroToIcebergSchemaVisitor(schema));
    Types.StructType icebergStruct = icebergSchema.asStruct();

    Types.NestedField myBytes = icebergStruct.field("myBytes");
    Assert.assertEquals(myBytes.fieldId(), 3);
  }

  @Test
  public void fooTest() {
    Schema schema = SchemaBuilder.record("foo")
        .fields()
        .name("bar")
        .type()
        .record("baz")
        .fields()
        .optionalDouble("number")
        .requiredLong("otherNumber")
        .endRecord()
        .noDefault()
        .optionalBytes("myBytes")
        .endRecord();

    com.netflix.iceberg.Schema schema1 = new com.netflix.iceberg.Schema(AvroSchemaUtil.convert(schema).asStructType().fields());
    System.out.println(schema1);
  }
}
