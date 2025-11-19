package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
}
