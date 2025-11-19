package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonArrayIdentifierExpressionTest {

  @Test
  void testOfWithVarargs() {
    JsonArrayIdentifierExpression expression =
        JsonArrayIdentifierExpression.of("attributes", "tags");

    assertEquals("attributes", expression.getColumnName());
    assertEquals(List.of("tags"), expression.getJsonPath());
  }

  @Test
  void testOfWithMultiplePathElements() {
    JsonArrayIdentifierExpression expression =
        JsonArrayIdentifierExpression.of("attributes", "nested", "array", "field");

    assertEquals("attributes", expression.getColumnName());
    assertEquals(List.of("nested", "array", "field"), expression.getJsonPath());
  }

  @Test
  void testOfWithList() {
    JsonArrayIdentifierExpression expression =
        JsonArrayIdentifierExpression.of("attributes", List.of("certifications"));

    assertEquals("attributes", expression.getColumnName());
    assertEquals(List.of("certifications"), expression.getJsonPath());
  }

  @Test
  void testOfWithNullPathElementsThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> JsonArrayIdentifierExpression.of("attributes", (String[]) null));

    assertEquals("JSON path cannot be null or empty for array field", exception.getMessage());
  }

  @Test
  void testOfWithEmptyPathElementsThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> JsonArrayIdentifierExpression.of("attributes"));

    assertEquals("JSON path cannot be null or empty for array field", exception.getMessage());
  }

  @Test
  void testOfWithNullListThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> JsonArrayIdentifierExpression.of("attributes", (List<String>) null));

    assertEquals("JSON path cannot be null or empty", exception.getMessage());
  }

  @Test
  void testOfWithEmptyListThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> JsonArrayIdentifierExpression.of("attributes", Collections.emptyList()));

    assertEquals("JSON path cannot be null or empty", exception.getMessage());
  }

  @Test
  void testEqualsAndHashCode() {
    JsonArrayIdentifierExpression expr1 = JsonArrayIdentifierExpression.of("attributes", "tags");
    JsonArrayIdentifierExpression expr2 = JsonArrayIdentifierExpression.of("attributes", "tags");

    // Test equality
    assertEquals(expr1, expr2, "Expressions with same column and path should be equal");

    // Test hashCode
    assertEquals(
        expr1.hashCode(),
        expr2.hashCode(),
        "Expressions with same column and path should have same hashCode");
  }

  @Test
  void testNotEqualsWithDifferentPath() {
    JsonArrayIdentifierExpression expr1 = JsonArrayIdentifierExpression.of("attributes", "tags");
    JsonArrayIdentifierExpression expr2 =
        JsonArrayIdentifierExpression.of("attributes", "categories");

    assertNotEquals(expr1, expr2, "Expressions with different paths should not be equal");
  }

  @Test
  void testNotEqualsWithDifferentColumn() {
    JsonArrayIdentifierExpression expr1 = JsonArrayIdentifierExpression.of("attributes", "tags");
    JsonArrayIdentifierExpression expr2 = JsonArrayIdentifierExpression.of("props", "tags");

    assertNotEquals(expr1, expr2, "Expressions with different columns should not be equal");
  }

  @Test
  void testNotEqualsWithJsonIdentifierExpression() {
    JsonArrayIdentifierExpression arrayExpr =
        JsonArrayIdentifierExpression.of("attributes", "tags");
    JsonIdentifierExpression scalarExpr = JsonIdentifierExpression.of("attributes", "tags");

    // Even though they have the same column and path, they are different types
    assertNotEquals(
        arrayExpr,
        scalarExpr,
        "JsonArrayIdentifierExpression should not equal JsonIdentifierExpression");
  }

  @Test
  void testInheritsFromJsonIdentifierExpression() {
    JsonArrayIdentifierExpression expression =
        JsonArrayIdentifierExpression.of("attributes", "tags");

    // Verify it's an instance of parent class
    assertEquals(
        JsonIdentifierExpression.class,
        expression.getClass().getSuperclass(),
        "JsonArrayIdentifierExpression should extend JsonIdentifierExpression");
  }

  @Test
  void testValidColumnNames() {
    // Valid column names should not throw
    assertDoesNotThrow(() -> JsonArrayIdentifierExpression.of("attributes", "tags"));
    assertDoesNotThrow(() -> JsonArrayIdentifierExpression.of("_internal", "field"));
    assertDoesNotThrow(() -> JsonArrayIdentifierExpression.of("customAttr", "array"));
    assertDoesNotThrow(() -> JsonArrayIdentifierExpression.of("attr123", "field"));
  }

  @Test
  void testInvalidColumnNameWithSqlInjection() {
    SecurityException exception =
        assertThrows(
            SecurityException.class,
            () -> JsonArrayIdentifierExpression.of("attributes\"; DROP TABLE users; --", "tags"));

    assertTrue(exception.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidColumnNameStartsWithNumber() {
    SecurityException exception =
        assertThrows(
            SecurityException.class, () -> JsonArrayIdentifierExpression.of("123attr", "tags"));

    assertTrue(exception.getMessage().contains("Must start with a letter or underscore"));
  }

  @Test
  void testInvalidColumnNameWithSpace() {
    SecurityException exception =
        assertThrows(
            SecurityException.class, () -> JsonArrayIdentifierExpression.of("my column", "tags"));

    assertTrue(exception.getMessage().contains("invalid"));
  }

  @Test
  void testValidJsonPathWithHyphen() {
    assertDoesNotThrow(() -> JsonArrayIdentifierExpression.of("attributes", "user-tags"));
    assertDoesNotThrow(() -> JsonArrayIdentifierExpression.of("attributes", "repo-urls"));
  }

  @Test
  void testValidJsonPathWithDot() {
    assertDoesNotThrow(() -> JsonArrayIdentifierExpression.of("attributes", "field.name"));
    assertDoesNotThrow(
        () -> JsonArrayIdentifierExpression.of("attributes", "user.preferences.tags"));
  }

  @Test
  void testInvalidJsonPathWithSqlInjection() {
    SecurityException exception =
        assertThrows(
            SecurityException.class,
            () -> JsonArrayIdentifierExpression.of("attributes", "tags' OR '1'='1"));

    assertTrue(exception.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPathWithSemicolon() {
    SecurityException exception =
        assertThrows(
            SecurityException.class,
            () -> JsonArrayIdentifierExpression.of("attributes", "field; DROP"));

    assertTrue(exception.getMessage().contains("invalid characters"));
  }

  @Test
  void testJsonPathMaxDepthExceeded() {
    String[] deepPath = new String[101]; // Max is 100
    for (int i = 0; i < deepPath.length; i++) {
      deepPath[i] = "level" + i;
    }

    SecurityException exception =
        assertThrows(
            SecurityException.class,
            () -> JsonArrayIdentifierExpression.of("attributes", deepPath));

    assertTrue(exception.getMessage().contains("exceeds maximum depth"));
  }

  @Test
  void testJsonPathWithEmptyElement() {
    SecurityException exception =
        assertThrows(
            SecurityException.class,
            () -> JsonArrayIdentifierExpression.of("attributes", "nested", "", "field"));

    assertTrue(exception.getMessage().contains("null or empty"));
  }

  @Test
  void testMultipleInstancesWithSamePathAreEqual() {
    JsonArrayIdentifierExpression expr1 =
        JsonArrayIdentifierExpression.of("attributes", "certifications");
    JsonArrayIdentifierExpression expr2 =
        JsonArrayIdentifierExpression.of("attributes", "certifications");
    JsonArrayIdentifierExpression expr3 =
        JsonArrayIdentifierExpression.of("attributes", "certifications");

    assertEquals(expr1, expr2);
    assertEquals(expr2, expr3);
    assertEquals(expr1, expr3);
    assertEquals(expr1.hashCode(), expr2.hashCode());
    assertEquals(expr2.hashCode(), expr3.hashCode());
  }

  @Test
  void testNestedArrayPath() {
    JsonArrayIdentifierExpression expression =
        JsonArrayIdentifierExpression.of("attributes", "nested", "deep", "arrays");

    assertEquals("attributes", expression.getColumnName());
    assertEquals(List.of("nested", "deep", "arrays"), expression.getJsonPath());
  }
}
