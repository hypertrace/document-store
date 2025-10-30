package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

public class JsonIdentifierExpressionSecurityTest {

  @Test
  void testValidExpressionSimpleField() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "brand"));
  }

  @Test
  void testValidExpressionNestedField() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "seller", "name"));
  }

  @Test
  void testValidExpressionDeeplyNested() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "seller", "address", "city"));
  }

  @Test
  void testValidExpressionWithNumbers() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "field123"));
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "1st_choice"));
  }

  @Test
  void testValidExpressionWithUnderscore() {
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("_internal", "field"));
    assertDoesNotThrow(() -> JsonIdentifierExpression.of("props", "_private"));
  }

  @Test
  void testValidExpressionUsingListConstructor() {
    assertDoesNotThrow(
        () -> JsonIdentifierExpression.of("props", List.of("seller", "address", "city")));
  }

  @Test
  void testInvalidExpressionColumnNameDropTable() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props\"; DROP TABLE users; --", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidExpressionColumnNameWithQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props\"name", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidExpressionColumnNameWithSemicolon() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props;SELECT", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidExpressionColumnNameStartsWithNumber() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("123props", "brand"));
    assertTrue(ex.getMessage().contains("Must start with a letter or underscore"));
  }

  @Test
  void testInvalidExpressionColumnNameWithSpace() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("my column", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidExpressionJsonPathWithQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props", "brand' OR '1'='1"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpressionJsonPathWithDoubleQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "name\"--"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpressionJsonPathWithSemicolon() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field; DROP"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpressionJsonPathWithHyphen() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field-name"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpressionJsonPathWithDot() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field.name"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpressionJsonPathWithSpace() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field name"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidExpression_sonPathEmptyElement() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props", "seller", "", "name"));
    assertTrue(ex.getMessage().contains("null or empty"));
  }

  @Test
  void testInvalidExpressionJsonPathTooDeep() {
    String[] deepPath = new String[11]; // Max is 10
    for (int i = 0; i < 11; i++) {
      deepPath[i] = "level" + i;
    }
    SecurityException ex =
        assertThrows(SecurityException.class, () -> JsonIdentifierExpression.of("props", deepPath));
    assertTrue(ex.getMessage().contains("exceeds maximum depth"));
  }

  @Test
  void testAttackScenarioSqlCommentInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props\"; -- comment", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testAttackScenarioUnionSelect() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () ->
                JsonIdentifierExpression.of(
                    "props\" UNION SELECT password FROM users WHERE \"x\"=\"x", "brand"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testAttackScenarioOrTrueInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props", "field' OR '1'='1' --"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testAttackScenarioNestedInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> JsonIdentifierExpression.of("props", "seller", "name'; DROP TABLE users; --"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testAttackScenarioSpecialCharacterCombination() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> JsonIdentifierExpression.of("props", "field'\"`;DROP"));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }
}
