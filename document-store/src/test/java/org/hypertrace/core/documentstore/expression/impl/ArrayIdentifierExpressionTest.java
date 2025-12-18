package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresArrayTypeExtractor;
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
  void testOfStringsCreatesInstanceWithTextType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofStrings("tags");

    assertEquals("tags", expression.getName());
    assertEquals(DataType.STRING, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    assertEquals("text[]", expression.accept(extractor));
  }

  @Test
  void testOfIntsCreatesInstanceWithIntegerType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofInts("ids");

    assertEquals("ids", expression.getName());
    assertEquals(DataType.INTEGER, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    assertEquals("integer[]", expression.accept(extractor));
  }

  @Test
  void testOfLongsCreatesInstanceWithBigintType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofLongs("timestamps");

    assertEquals("timestamps", expression.getName());
    assertEquals(DataType.LONG, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    assertEquals("bigint[]", expression.accept(extractor));
  }

  @Test
  void testOfFloatsCreatesInstanceWithFloatType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofFloats("temperatures");

    assertEquals("temperatures", expression.getName());
    assertEquals(DataType.FLOAT, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    assertEquals("real[]", expression.accept(extractor));
  }

  @Test
  void testOfDoublesCreatesInstanceWithDoubleType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofDoubles("coordinates");

    assertEquals("coordinates", expression.getName());
    assertEquals(DataType.DOUBLE, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    assertEquals("double precision[]", expression.accept(extractor));
  }

  @Test
  void testOfBooleansCreatesInstanceWithBooleanType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofBooleans("flags");

    assertEquals("flags", expression.getName());
    assertEquals(DataType.BOOLEAN, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    assertEquals("boolean[]", expression.accept(extractor));
  }

  @Test
  void testOfTimestampsTzCreatesInstanceWithTimestampTzType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofTimestampsTz("eventsTz");

    assertEquals("eventsTz", expression.getName());
    assertEquals(DataType.TIMESTAMPTZ, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    assertEquals("timestamptz[]", expression.accept(extractor));
  }

  @Test
  void testOfDatesCreatesInstanceWithDateType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofDates("dates");

    assertEquals("dates", expression.getName());
    assertEquals(DataType.DATE, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    assertEquals("date[]", expression.accept(extractor));
  }

  @Test
  void testPostgresArrayTypeExtractorReturnsNullForUnspecifiedType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.of("untyped");

    assertEquals(DataType.UNSPECIFIED, expression.getElementDataType());

    PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();
    String arrayType = expression.accept(extractor);
    assertNull(arrayType);
  }
}
