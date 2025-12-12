package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PostgresDataTypeTest {

  @Test
  void testTextGetType() {
    assertEquals("text", PostgresDataType.TEXT.getType());
  }

  @Test
  void testTextToString() {
    assertEquals("text", PostgresDataType.TEXT.toString());
  }

  @Test
  void testIntegerGetType() {
    assertEquals("int4", PostgresDataType.INTEGER.getType());
  }

  @Test
  void testIntegerToString() {
    assertEquals("int4", PostgresDataType.INTEGER.toString());
  }

  @Test
  void testBigintGetType() {
    assertEquals("int8", PostgresDataType.BIGINT.getType());
  }

  @Test
  void testBigintToString() {
    assertEquals("int8", PostgresDataType.BIGINT.toString());
  }

  @Test
  void testSmallintGetType() {
    assertEquals("int2", PostgresDataType.SMALLINT.getType());
  }

  @Test
  void testSmallintToString() {
    assertEquals("int2", PostgresDataType.SMALLINT.toString());
  }

  @Test
  void testFloatGetType() {
    assertEquals("float4", PostgresDataType.FLOAT.getType());
  }

  @Test
  void testFloatToString() {
    assertEquals("float4", PostgresDataType.FLOAT.toString());
  }

  @Test
  void testDoubleGetType() {
    assertEquals("float8", PostgresDataType.DOUBLE.getType());
  }

  @Test
  void testDoubleToString() {
    assertEquals("float8", PostgresDataType.DOUBLE.toString());
  }

  @Test
  void testNumericGetType() {
    assertEquals("numeric", PostgresDataType.NUMERIC.getType());
  }

  @Test
  void testNumericToString() {
    assertEquals("numeric", PostgresDataType.NUMERIC.toString());
  }

  @Test
  void testBooleanGetType() {
    assertEquals("bool", PostgresDataType.BOOLEAN.getType());
  }

  @Test
  void testBooleanToString() {
    assertEquals("bool", PostgresDataType.BOOLEAN.toString());
  }

  @Test
  void testTimestampGetType() {
    assertEquals("timestamp", PostgresDataType.TIMESTAMP.getType());
  }

  @Test
  void testTimestampToString() {
    assertEquals("timestamp", PostgresDataType.TIMESTAMP.toString());
  }

  @Test
  void testTimestamptzGetType() {
    assertEquals("timestamptz", PostgresDataType.TIMESTAMPTZ.getType());
  }

  @Test
  void testTimestamptzToString() {
    assertEquals("timestamptz", PostgresDataType.TIMESTAMPTZ.toString());
  }

  @Test
  void testDateGetType() {
    assertEquals("date", PostgresDataType.DATE.getType());
  }

  @Test
  void testDateToString() {
    assertEquals("date", PostgresDataType.DATE.toString());
  }

  @Test
  void testUuidGetType() {
    assertEquals("uuid", PostgresDataType.UUID.getType());
  }

  @Test
  void testUuidToString() {
    assertEquals("uuid", PostgresDataType.UUID.toString());
  }

  @Test
  void testJsonbGetType() {
    assertEquals("jsonb", PostgresDataType.JSONB.getType());
  }

  @Test
  void testJsonbToString() {
    assertEquals("jsonb", PostgresDataType.JSONB.toString());
  }

  @Test
  void testByteaGetType() {
    assertEquals("bytea", PostgresDataType.BYTEA.getType());
  }

  @Test
  void testByteaToString() {
    assertEquals("bytea", PostgresDataType.BYTEA.toString());
  }

  @Test
  void testGetTypeAndToStringReturnSameValue() {
    // Verify that getType() and toString() always return the same value for all enum constants
    for (PostgresDataType type : PostgresDataType.values()) {
      assertEquals(
          type.getType(),
          type.toString(),
          "getType() and toString() should return the same value for " + type.name());
    }
  }

  @Test
  void testGetPgTypeNameMatchesGetType() {
    // Verify that getPgTypeName() returns the same as getType()
    for (PostgresDataType type : PostgresDataType.values()) {
      assertEquals(
          type.getType(),
          type.getPgTypeName(),
          "getPgTypeName() should match getType() for " + type.name());
    }
  }

  @Test
  void testAllEnumValuesHaveNonNullType() {
    // Verify that all enum constants have non-null type names
    for (PostgresDataType type : PostgresDataType.values()) {
      assertEquals(type.getType() != null, true, "Type name should not be null for " + type.name());
    }
  }
}
