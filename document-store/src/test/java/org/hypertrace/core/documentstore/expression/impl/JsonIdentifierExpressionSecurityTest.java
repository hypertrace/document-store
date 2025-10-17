package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Security tests for JsonIdentifierExpression to ensure SQL injection prevention. */
public class JsonIdentifierExpressionSecurityTest {

  // ===== Valid Expressions =====

  @Test
  void testValidExpression_SimpleField() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "brand"));
  }

  @Test
  void testValidExpression_NestedField() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "seller", "name"));
  }

  @Test
  void testValidExpression_DeeplyNested() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "seller", "address", "city"));
  }

  @Test
  void testValidExpression_WithNumbers() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "field123"));
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "1st_choice"));
  }

  @Test
  void testValidExpression_WithUnderscore() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("_internal", "field"));
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "_private"));
  }

  @Test
  void testValidExpression_UsingListConstructor() {
    assertDoesNotThrow(
        () -> JsonIdentifierExpression.of("props", List.of("seller", "address", "city")));
  }

  // ===== Invalid Column Names =====

  @Test
  void testInvalidExpression_ColumnName_DropTable() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props\"; DROP TABLE users; --", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidExpression_ColumnName_WithQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props\"name", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidExpression_ColumnName_WithSemicolon() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props;SELECT", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidExpression_ColumnName_StartsWithNumber() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("123props", "brand"));
    assertTrue(ex.getMessage().contains("Must start with a letter or underscore"));
  }

  @Test
  void testInvalidExpression_ColumnName_WithHyphen() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("my-column", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidExpression_ColumnName_WithSpace() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("my column", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  // ===== Invalid JSON Paths =====

  @Test
  void testInvalidExpression_JsonPath_WithQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props", "brand' OR '1'='1"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpression_JsonPath_WithDoubleQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "name\"--"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpression_JsonPath_WithSemicolon() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field; DROP"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpression_JsonPath_WithHyphen() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field-name"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpression_JsonPath_WithDot() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field.name"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpression_JsonPath_WithSpace() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field name"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpression_JsonPath_EmptyElement() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props", "seller", "", "name"));
    assertTrue(ex.getMessage().contains("null or empty"));
  }

  @Test
  void testInvalidExpression_JsonPath_TooDeep() {
    String[] deepPath = new String[11]; // Max is 10
    for (int i = 0; i < 11; i++) {
      deepPath[i] = "level" + i;
    }
    SecurityException ex =
        assertThrows(SecurityException.class, () -> JsonIdentifierExpression.of("props", deepPath));
    assertTrue(ex.getMessage().contains("exceeds maximum depth"));
  }

  // ===== Real-world Attack Scenarios =====

  @Test
  void testAttackScenario_SqlCommentInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props\"; -- comment", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testAttackScenario_UnionSelect() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () ->
                JsonIdentifierExpression.of(
                    "props\" UNION SELECT password FROM users WHERE \"x\"=\"x", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testAttackScenario_OrTrueInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props", "field' OR '1'='1' --"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testAttackScenario_NestedInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props", "seller", "name'; DROP TABLE users; --"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testAttackScenario_SpecialCharacterCombination() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field'\"`;DROP"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }
}
