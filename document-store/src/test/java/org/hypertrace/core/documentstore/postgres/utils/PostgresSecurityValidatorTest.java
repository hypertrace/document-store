package org.hypertrace.core.documentstore.postgres.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Security tests for BasicPostgresSecurityValidator to prevent SQL injection attacks. */
public class PostgresSecurityValidatorTest {

  private final PostgresSecurityValidator validator = BasicPostgresSecurityValidator.getDefault();

  // ===== Column/Identifier Validation Tests =====

  @Test
  void testValidIdentifier_Letters() {
    assertDoesNotThrow(() -> validator.validateIdentifier("props"));
    assertDoesNotThrow(() -> validator.validateIdentifier("myColumn"));
    assertDoesNotThrow(() -> validator.validateIdentifier("UPPERCASE"));
  }

  @Test
  void testValidIdentifier_StartsWithUnderscore() {
    assertDoesNotThrow(() -> validator.validateIdentifier("_internal"));
    assertDoesNotThrow(() -> validator.validateIdentifier("_"));
  }

  @Test
  void testValidIdentifier_WithNumbers() {
    assertDoesNotThrow(() -> validator.validateIdentifier("field123"));
    assertDoesNotThrow(() -> validator.validateIdentifier("col_1"));
  }

  @Test
  void testInvalidIdentifier_Null() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier(null));
    assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  void testInvalidIdentifier_Empty() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier(""));
    assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  void testInvalidIdentifier_StartsWithNumber() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("123column"));
    assertTrue(ex.getMessage().contains("Must start with a letter or underscore"));
  }

  @Test
  void testInvalidIdentifier_SqlInjection_DropTable() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> validator.validateIdentifier("props\"; DROP TABLE users; --"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifier_SqlInjection_Quote() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("column\"name"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifier_SqlInjection_Semicolon() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("col;SELECT"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifier_Hyphen() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("field-name"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifier_Dot() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("field.name"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifier_Space() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("field name"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifier_TooLong() {
    String longIdentifier = "a".repeat(64); // PostgreSQL max is 63
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier(longIdentifier));
    assertTrue(ex.getMessage().contains("exceeds maximum length"));
  }

  @Test
  void testValidIdentifier_MaxLength() {
    String maxLengthIdentifier = "a".repeat(63); // Exactly at limit
    assertDoesNotThrow(() -> validator.validateIdentifier(maxLengthIdentifier));
  }

  // ===== JSON Path Validation Tests =====

  @Test
  void testValidJsonPath_SingleLevel() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("brand")));
  }

  @Test
  void testValidJsonPath_MultiLevel() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("seller", "address", "city")));
  }

  @Test
  void testValidJsonPath_WithNumbers() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("field123")));
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("1st_choice")));
  }

  @Test
  void testValidJsonPath_StartsWithUnderscore() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("_private")));
  }

  @Test
  void testInvalidJsonPath_Null() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(null));
    assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  void testInvalidJsonPath_Empty() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(List.of()));
    assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  void testInvalidJsonPath_NullElement() {
    // Note: List.of() doesn't allow null elements, so we need to create a list differently
    java.util.List<String> pathWithNull = new java.util.ArrayList<>();
    pathWithNull.add("field");
    pathWithNull.add(null);
    pathWithNull.add("name");

    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(pathWithNull));
    assertTrue(ex.getMessage().contains("null or empty"));
  }

  @Test
  void testInvalidJsonPath_EmptyElement() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> validator.validateJsonPath(List.of("field", "", "name")));
    assertTrue(ex.getMessage().contains("null or empty"));
  }

  @Test
  void testInvalidJsonPath_SqlInjection_Quote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("brand' OR '1'='1")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPath_SqlInjection_DoubleQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("name\"--")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPath_SqlInjection_Semicolon() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> validator.validateJsonPath(List.of("field; DROP TABLE")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPath_Hyphen() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("field-name")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPath_Dot() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("field.name")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPath_Space() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("field name")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPath_TooDeep() {
    List<String> deepPath = new ArrayList<>();
    for (int i = 0; i < 11; i++) { // Max is 10
      deepPath.add("level" + i);
    }
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(deepPath));
    assertTrue(ex.getMessage().contains("exceeds maximum depth"));
  }

  @Test
  void testValidJsonPath_MaxDepth() {
    List<String> deepPath = new ArrayList<>();
    for (int i = 0; i < 10; i++) { // Exactly at limit
      deepPath.add("level" + i);
    }
    assertDoesNotThrow(() -> validator.validateJsonPath(deepPath));
  }

  @Test
  void testInvalidJsonPath_FieldTooLong() {
    String longField = "a".repeat(101); // Max is 100
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(List.of(longField)));
    assertTrue(ex.getMessage().contains("exceeds maximum length"));
  }

  @Test
  void testValidJsonPath_MaxFieldLength() {
    String maxLengthField = "a".repeat(100); // Exactly at limit
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of(maxLengthField)));
  }

  // ===== Real-world Attack Scenarios =====

  @Test
  void testAttackScenario_CommentInjection() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("field\"--"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testAttackScenario_UnionInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateIdentifier("col\" UNION SELECT"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testAttackScenario_OrInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> validator.validateJsonPath(List.of("field\" OR \"1\"=\"1")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testAttackScenario_NestedQuotes() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("field'\"'; DROP")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }
}
