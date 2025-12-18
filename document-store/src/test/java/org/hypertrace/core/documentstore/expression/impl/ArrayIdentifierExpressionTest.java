package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresTypeExtractor;
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

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    assertEquals("text[]", expression.accept(extractor));
  }

  @Test
  void testOfIntsCreatesInstanceWithIntegerType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofInts("ids");

    assertEquals("ids", expression.getName());
    assertEquals(DataType.INTEGER, expression.getElementDataType());

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    assertEquals("int4[]", expression.accept(extractor));
  }

  @Test
  void testOfLongsCreatesInstanceWithBigintType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofLongs("timestamps");

    assertEquals("timestamps", expression.getName());
    assertEquals(DataType.LONG, expression.getElementDataType());

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    assertEquals("int8[]", expression.accept(extractor));
  }

  @Test
  void testOfFloatsCreatesInstanceWithFloatType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofFloats("temperatures");

    assertEquals("temperatures", expression.getName());
    assertEquals(DataType.FLOAT, expression.getElementDataType());

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    assertEquals("float4[]", expression.accept(extractor));
  }

  @Test
  void testOfDoublesCreatesInstanceWithDoubleType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofDoubles("coordinates");

    assertEquals("coordinates", expression.getName());
    assertEquals(DataType.DOUBLE, expression.getElementDataType());

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    assertEquals("float8[]", expression.accept(extractor));
  }

  @Test
  void testOfBooleansCreatesInstanceWithBooleanType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofBooleans("flags");

    assertEquals("flags", expression.getName());
    assertEquals(DataType.BOOLEAN, expression.getElementDataType());

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    assertEquals("bool[]", expression.accept(extractor));
  }

  @Test
  void testOfTimestampsTzCreatesInstanceWithTimestampTzType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofTimestampsTz("eventsTz");

    assertEquals("eventsTz", expression.getName());
    assertEquals(DataType.TIMESTAMPTZ, expression.getElementDataType());

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    assertEquals("timestamptz[]", expression.accept(extractor));
  }

  @Test
  void testOfDatesCreatesInstanceWithDateType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.ofDates("dates");

    assertEquals("dates", expression.getName());
    assertEquals(DataType.DATE, expression.getElementDataType());

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    assertEquals("date[]", expression.accept(extractor));
  }

  @Test
  void testPostgresTypeExtractorReturnsNullForUnspecifiedType() {
    ArrayIdentifierExpression expression = ArrayIdentifierExpression.of("untyped");

    assertEquals(DataType.UNSPECIFIED, expression.getElementDataType());

    PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();
    String arrayType = expression.accept(extractor);
    assertNull(arrayType);
  }
}
