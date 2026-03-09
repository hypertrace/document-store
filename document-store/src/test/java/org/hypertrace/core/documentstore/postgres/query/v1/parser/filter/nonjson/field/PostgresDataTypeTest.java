package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PostgresDataTypeTest {

  @Nested
  @DisplayName("getTypeCast Tests")
  class GetTypeCastTests {

    @Test
    @DisplayName("TEXT should return ::text")
    void testTextTypeCast() {
      assertEquals("::text", PostgresDataType.TEXT.getTypeCast());
    }

    @Test
    @DisplayName("INTEGER should return ::int4")
    void testIntegerTypeCast() {
      assertEquals("::int4", PostgresDataType.INTEGER.getTypeCast());
    }

    @Test
    @DisplayName("BIGINT should return ::int8")
    void testBigintTypeCast() {
      assertEquals("::int8", PostgresDataType.BIGINT.getTypeCast());
    }

    @Test
    @DisplayName("REAL should return ::float4")
    void testRealTypeCast() {
      assertEquals("::float4", PostgresDataType.REAL.getTypeCast());
    }

    @Test
    @DisplayName("DOUBLE_PRECISION should return ::float8")
    void testDoublePrecisionTypeCast() {
      assertEquals("::float8", PostgresDataType.DOUBLE_PRECISION.getTypeCast());
    }

    @Test
    @DisplayName("BOOLEAN should return ::bool")
    void testBooleanTypeCast() {
      assertEquals("::bool", PostgresDataType.BOOLEAN.getTypeCast());
    }

    @Test
    @DisplayName("JSONB should return ::jsonb")
    void testJsonbTypeCast() {
      assertEquals("::jsonb", PostgresDataType.JSONB.getTypeCast());
    }

    @Test
    @DisplayName("TIMESTAMPTZ should return ::timestamptz")
    void testTimestamptzTypeCast() {
      assertEquals("::timestamptz", PostgresDataType.TIMESTAMPTZ.getTypeCast());
    }

    @Test
    @DisplayName("DATE should return ::date")
    void testDateTypeCast() {
      assertEquals("::date", PostgresDataType.DATE.getTypeCast());
    }

    @Test
    @DisplayName("UNKNOWN (null sqlType) should return empty string")
    void testUnknownTypeCastReturnsEmpty() {
      assertEquals("", PostgresDataType.UNKNOWN.getTypeCast());
    }
  }

  @Nested
  @DisplayName("getArrayTypeCast Tests")
  class GetArrayTypeCastTests {

    @Test
    @DisplayName("TEXT should return ::text[]")
    void testTextArrayTypeCast() {
      assertEquals("::text[]", PostgresDataType.TEXT.getArrayTypeCast());
    }

    @Test
    @DisplayName("INTEGER should return ::int4[]")
    void testIntegerArrayTypeCast() {
      assertEquals("::int4[]", PostgresDataType.INTEGER.getArrayTypeCast());
    }

    @Test
    @DisplayName("BIGINT should return ::int8[]")
    void testBigintArrayTypeCast() {
      assertEquals("::int8[]", PostgresDataType.BIGINT.getArrayTypeCast());
    }

    @Test
    @DisplayName("REAL should return ::float4[]")
    void testRealArrayTypeCast() {
      assertEquals("::float4[]", PostgresDataType.REAL.getArrayTypeCast());
    }

    @Test
    @DisplayName("DOUBLE_PRECISION should return ::float8[]")
    void testDoublePrecisionArrayTypeCast() {
      assertEquals("::float8[]", PostgresDataType.DOUBLE_PRECISION.getArrayTypeCast());
    }

    @Test
    @DisplayName("BOOLEAN should return ::bool[]")
    void testBooleanArrayTypeCast() {
      assertEquals("::bool[]", PostgresDataType.BOOLEAN.getArrayTypeCast());
    }

    @Test
    @DisplayName("UNKNOWN (null sqlType) should return empty string")
    void testUnknownArrayTypeCastReturnsEmpty() {
      assertEquals("", PostgresDataType.UNKNOWN.getArrayTypeCast());
    }
  }

  @Nested
  @DisplayName("getJsonArrayElementTypeCast Tests")
  class GetJsonArrayElementTypeCastTests {

    @Test
    @DisplayName("STRING_ARRAY should return ::text")
    void testStringArrayTypeCast() {
      assertEquals(
          "::text", PostgresDataType.getJsonArrayElementTypeCast(JsonFieldType.STRING_ARRAY));
    }

    @Test
    @DisplayName("NUMBER_ARRAY should return ::numeric")
    void testNumberArrayTypeCast() {
      assertEquals(
          "::numeric", PostgresDataType.getJsonArrayElementTypeCast(JsonFieldType.NUMBER_ARRAY));
    }

    @Test
    @DisplayName("BOOLEAN_ARRAY should return ::boolean")
    void testBooleanArrayTypeCast() {
      assertEquals(
          "::boolean", PostgresDataType.getJsonArrayElementTypeCast(JsonFieldType.BOOLEAN_ARRAY));
    }

    @Test
    @DisplayName("OBJECT_ARRAY should return ::jsonb")
    void testObjectArrayTypeCast() {
      assertEquals(
          "::jsonb", PostgresDataType.getJsonArrayElementTypeCast(JsonFieldType.OBJECT_ARRAY));
    }

    @Test
    @DisplayName("Non-array type should throw IllegalArgumentException")
    void testNonArrayTypeThrowsException() {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> PostgresDataType.getJsonArrayElementTypeCast(JsonFieldType.STRING));

      assertEquals(
          "Unsupported array type: STRING. Expected *_ARRAY types.", exception.getMessage());
    }

    @Test
    @DisplayName("NUMBER type should throw IllegalArgumentException")
    void testNumberTypeThrowsException() {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> PostgresDataType.getJsonArrayElementTypeCast(JsonFieldType.NUMBER));

      assertEquals(
          "Unsupported array type: NUMBER. Expected *_ARRAY types.", exception.getMessage());
    }
  }
}
