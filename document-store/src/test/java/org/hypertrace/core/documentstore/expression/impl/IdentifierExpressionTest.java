package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class IdentifierExpressionTest {

  @Test
  void testOfCreatesInstanceWithoutType() {
    IdentifierExpression expression = IdentifierExpression.of("column1");

    assertEquals("column1", expression.getName());
    assertNull(expression.getFlatCollectionDataType());
  }

  @Test
  void testOfBytesCreatesInstanceWithBytesType() {
    IdentifierExpression expression = IdentifierExpression.ofBytes("binaryData");

    assertEquals("binaryData", expression.getName());
    assertNotNull(expression.getFlatCollectionDataType());
    assertEquals(PostgresDataType.BYTEA, expression.getFlatCollectionDataType());
  }

  @Test
  void testOfBytesEquality() {
    IdentifierExpression expr1 = IdentifierExpression.ofBytes("data");
    IdentifierExpression expr2 = IdentifierExpression.ofBytes("data");

    assertEquals(expr1, expr2);
    assertEquals(expr1.hashCode(), expr2.hashCode());
  }

  @Test
  void testOfBytesInequality() {
    IdentifierExpression expr1 = IdentifierExpression.ofBytes("data1");
    IdentifierExpression expr2 = IdentifierExpression.ofBytes("data2");

    assertNotEquals(expr1, expr2);
  }

  @Test
  void testOfBytesDifferentFromUntyped() {
    IdentifierExpression typedExpr = IdentifierExpression.ofBytes("column");
    IdentifierExpression untypedExpr = IdentifierExpression.of("column");

    assertNotEquals(typedExpr, untypedExpr);
  }

  @Test
  void testOfStringCreatesInstanceWithTextType() {
    IdentifierExpression expression = IdentifierExpression.ofString("name");

    assertEquals("name", expression.getName());
    assertEquals(PostgresDataType.TEXT, expression.getFlatCollectionDataType());
  }

  @Test
  void testOfIntCreatesInstanceWithIntegerType() {
    IdentifierExpression expression = IdentifierExpression.ofInt("age");

    assertEquals("age", expression.getName());
    assertEquals(PostgresDataType.INTEGER, expression.getFlatCollectionDataType());
  }

  @Test
  void testOfLongCreatesInstanceWithBigintType() {
    IdentifierExpression expression = IdentifierExpression.ofLong("timestamp");

    assertEquals("timestamp", expression.getName());
    assertEquals(PostgresDataType.BIGINT, expression.getFlatCollectionDataType());
  }

  @Test
  void testOfBooleanCreatesInstanceWithBooleanType() {
    IdentifierExpression expression = IdentifierExpression.ofBoolean("isActive");

    assertEquals("isActive", expression.getName());
    assertEquals(PostgresDataType.BOOLEAN, expression.getFlatCollectionDataType());
  }

  @Test
  void testMultipleTypedExpressionsAreEqualWithSameNameAndType() {
    IdentifierExpression expr1 = IdentifierExpression.ofBytes("file");
    IdentifierExpression expr2 = IdentifierExpression.ofBytes("file");
    IdentifierExpression expr3 = IdentifierExpression.ofBytes("file");

    assertEquals(expr1, expr2);
    assertEquals(expr2, expr3);
    assertEquals(expr1, expr3);
    assertEquals(expr1.hashCode(), expr2.hashCode());
    assertEquals(expr2.hashCode(), expr3.hashCode());
  }

  @Test
  void testDifferentTypesWithSameNameAreNotEqual() {
    IdentifierExpression bytesExpr = IdentifierExpression.ofBytes("data");
    IdentifierExpression stringExpr = IdentifierExpression.ofString("data");

    assertNotEquals(bytesExpr, stringExpr);
  }
}
