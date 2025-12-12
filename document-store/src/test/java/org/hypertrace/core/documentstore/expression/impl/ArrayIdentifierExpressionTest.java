package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class ArrayIdentifierExpressionTest {

  @Test
  void testOfCreatesInstance() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.of("tags");

    assertEquals("tags", expression.getName());
  }

  @Test
  void testEqualsAndHashCode() {
    ArrayIdentifierExpression expr1 = ArrayIdentifierExpression.of("tags");
    ArrayIdentifierExpression expr2 = ArrayIdentifierExpression.of("tags");

    // Test equality - should be equal
    assertEquals(expr1, expr2, "Expressions with same name should be equal");

    // Test hashCode
    assertEquals(
        expr1.hashCode(), expr2.hashCode(), "Expressions with same name should have same hashCode");
  }

  @Test
  void testNotEqualsWithDifferentName() {
    ArrayIdentifierExpression expr1 = ArrayIdentifierExpression.of("tags");
    ArrayIdentifierExpression expr2 = ArrayIdentifierExpression.of("categories");

    // Test inequality
    assertNotEquals(expr1, expr2, "Expressions with different names should not be equal");
  }

  @Test
  void testNotEqualsWithIdentifierExpression() {
    ArrayIdentifierExpression arrayExpr = ArrayIdentifierExpression.of("tags");
    IdentifierExpression identExpr = IdentifierExpression.of("tags");

    // Even though they have the same name, they are different types
    assertNotEquals(
        arrayExpr, identExpr, "ArrayIdentifierExpression should not equal IdentifierExpression");
  }

  @Test
  void testInheritsFromIdentifierExpression() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.of("tags");

    // Verify it's an instance of parent class
    assertEquals(
        IdentifierExpression.class,
        expression.getClass().getSuperclass(),
        "ArrayIdentifierExpression should extend IdentifierExpression");
  }

  @Test
  void testMultipleInstancesWithSameNameAreEqual() {
    ArrayIdentifierExpression expr1 = ArrayIdentifierExpression.of("categoryTags");
    ArrayIdentifierExpression expr2 = ArrayIdentifierExpression.of("categoryTags");
    ArrayIdentifierExpression expr3 = ArrayIdentifierExpression.of("categoryTags");

    assertEquals(expr1, expr2);
    assertEquals(expr2, expr3);
    assertEquals(expr1, expr3);
    assertEquals(expr1.hashCode(), expr2.hashCode());
    assertEquals(expr2.hashCode(), expr3.hashCode());
  }

  @Test
  void testOfBytesArrayCreatesInstanceWithBytesType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofBytesArray("binaryData");

    assertEquals("binaryData", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.BYTEA, expression.getArrayElementType().get());
  }

  @Test
  void testOfBytesArrayReturnsCorrectPostgresArrayType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofBytesArray("fileContent");

    Optional<String> arrayType = expression.getPostgresArrayTypeString();
    assertTrue(arrayType.isPresent());
    assertEquals("bytea[]", arrayType.get());
  }

  @Test
  void testOfBytesArrayEquality() {
    ArrayIdentifierExpression expr1 = ArrayIdentifierExpression.ofBytesArray("data");
    ArrayIdentifierExpression expr2 = ArrayIdentifierExpression.ofBytesArray("data");

    assertEquals(expr1, expr2);
    assertEquals(expr1.hashCode(), expr2.hashCode());
  }

  @Test
  void testOfBytesArrayInequality() {
    ArrayIdentifierExpression expr1 = ArrayIdentifierExpression.ofBytesArray("data1");
    ArrayIdentifierExpression expr2 = ArrayIdentifierExpression.ofBytesArray("data2");

    assertNotEquals(expr1, expr2);
  }

  @Test
  void testOfStringsCreatesInstanceWithTextType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofStrings("tags");

    assertEquals("tags", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.TEXT, expression.getArrayElementType().get());
    assertEquals("text[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfIntsCreatesInstanceWithIntegerType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofInts("ids");

    assertEquals("ids", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.INTEGER, expression.getArrayElementType().get());
    assertEquals("integer[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfLongsCreatesInstanceWithBigintType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofLongs("timestamps");

    assertEquals("timestamps", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.BIGINT, expression.getArrayElementType().get());
    assertEquals("bigint[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfShortsCreatesInstanceWithSmallintType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofShorts("counts");

    assertEquals("counts", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.SMALLINT, expression.getArrayElementType().get());
    assertEquals("smallint[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfFloatsCreatesInstanceWithFloatType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofFloats("temperatures");

    assertEquals("temperatures", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.FLOAT, expression.getArrayElementType().get());
    assertEquals("real[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfDoublesCreatesInstanceWithDoubleType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofDoubles("coordinates");

    assertEquals("coordinates", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.DOUBLE, expression.getArrayElementType().get());
    assertEquals("double precision[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfDecimalsCreatesInstanceWithNumericType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofDecimals("prices");

    assertEquals("prices", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.NUMERIC, expression.getArrayElementType().get());
    assertEquals("numeric[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfBooleansCreatesInstanceWithBooleanType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofBooleans("flags");

    assertEquals("flags", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.BOOLEAN, expression.getArrayElementType().get());
    assertEquals("boolean[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfTimestampsCreatesInstanceWithTimestampType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofTimestamps("events");

    assertEquals("events", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.TIMESTAMP, expression.getArrayElementType().get());
    assertEquals("timestamp[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfTimestampsTzCreatesInstanceWithTimestampTzType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofTimestampsTz("eventsTz");

    assertEquals("eventsTz", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.TIMESTAMPTZ, expression.getArrayElementType().get());
    assertEquals("timestamptz[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfDatesCreatesInstanceWithDateType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofDates("dates");

    assertEquals("dates", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.DATE, expression.getArrayElementType().get());
    assertEquals("date[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testOfUuidsCreatesInstanceWithUuidType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofUuids("uuids");

    assertEquals("uuids", expression.getName());
    assertTrue(expression.getArrayElementType().isPresent());
    assertEquals(PostgresDataType.UUID, expression.getArrayElementType().get());
    assertEquals("uuid[]", expression.getPostgresArrayTypeString().orElse(null));
  }

  @Test
  void testToPostgresArrayTypeThrowsExceptionForNonPostgresDataType() throws Exception {
    // Create an expression with a custom FlatCollectionDataType that is not PostgresDataType
    FlatCollectionDataType customType =
        new FlatCollectionDataType() {
          @Override
          public String getType() {
            return "custom_type";
          }
        };

    ArrayIdentifierExpression expression = new ArrayIdentifierExpression("test", customType);

    // Calling getPostgresArrayTypeString should throw IllegalArgumentException
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> expression.getPostgresArrayTypeString());

    assertTrue(exception.getMessage().contains("Only PostgresDataType is currently supported"));
  }

  @Test
  void testGetPostgresArrayTypeStringReturnsEmptyForNullType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.of("untyped");

    Optional<String> arrayType = expression.getPostgresArrayTypeString();
    assertTrue(arrayType.isEmpty());
  }
}
