package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class IdentifierExpressionTest {

  @Test
  void testOfCreatesInstanceWithUnspecifiedType() {
    IdentifierExpression expression = IdentifierExpression.of("column1");

    assertEquals("column1", expression.getName());
    assertEquals(DataType.UNSPECIFIED, expression.getDataType());
  }

  @Test
  void testOfStringCreatesInstanceWithTextType() {
    IdentifierExpression expression = IdentifierExpression.ofString("name");

    assertEquals("name", expression.getName());
    assertEquals(DataType.STRING, expression.getDataType());
  }

  @Test
  void testOfIntCreatesInstanceWithIntegerType() {
    IdentifierExpression expression = IdentifierExpression.ofInt("age");

    assertEquals("age", expression.getName());
    assertEquals(DataType.INTEGER, expression.getDataType());
  }

  @Test
  void testOfLongCreatesInstanceWithBigintType() {
    IdentifierExpression expression = IdentifierExpression.ofLong("timestamp");

    assertEquals("timestamp", expression.getName());
    assertEquals(DataType.LONG, expression.getDataType());
  }

  @Test
  void testOfBooleanCreatesInstanceWithBooleanType() {
    IdentifierExpression expression = IdentifierExpression.ofBoolean("isActive");

    assertEquals("isActive", expression.getName());
    assertEquals(DataType.BOOLEAN, expression.getDataType());
  }

  @Test
  void testOfFloatCreatesInstanceWithFloatType() {
    IdentifierExpression expression = IdentifierExpression.ofFloat("temperature");

    assertEquals("temperature", expression.getName());
    assertEquals(DataType.FLOAT, expression.getDataType());
  }

  @Test
  void testOfDoubleCreatesInstanceWithDoubleType() {
    IdentifierExpression expression = IdentifierExpression.ofDouble("latitude");

    assertEquals("latitude", expression.getName());
    assertEquals(DataType.DOUBLE, expression.getDataType());
  }

  @Test
  void testOfTimestampTzCreatesInstanceWithTimestampTzType() {
    IdentifierExpression expression = IdentifierExpression.ofTimestampTz("updatedAt");

    assertEquals("updatedAt", expression.getName());
    assertEquals(DataType.TIMESTAMPTZ, expression.getDataType());
  }

  @Test
  void testOfDateCreatesInstanceWithDateType() {
    IdentifierExpression expression = IdentifierExpression.ofDate("birthDate");

    assertEquals("birthDate", expression.getName());
    assertEquals(DataType.DATE, expression.getDataType());
  }

  @Test
  void testMultipleTypedExpressionsAreEqualWithSameNameAndType() {
    IdentifierExpression expr1 = IdentifierExpression.ofString("file");
    IdentifierExpression expr2 = IdentifierExpression.ofString("file");
    IdentifierExpression expr3 = IdentifierExpression.ofString("file");

    assertEquals(expr1, expr2);
    assertEquals(expr2, expr3);
    assertEquals(expr1, expr3);
    assertEquals(expr1.hashCode(), expr2.hashCode());
    assertEquals(expr2.hashCode(), expr3.hashCode());
  }

  @Test
  void testDifferentTypesWithSameNameAreNotEqual() {
    IdentifierExpression intExpr = IdentifierExpression.ofInt("data");
    IdentifierExpression stringExpr = IdentifierExpression.ofString("data");

    assertNotEquals(intExpr, stringExpr);
  }
}
