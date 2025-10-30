package org.hypertrace.core.documentstore.postgres.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PostgresSecurityValidatorTest {

  private final PostgresSecurityValidator validator = BasicPostgresSecurityValidator.getDefault();

  @Test
  void testValidIdentifierLetters() {
    assertDoesNotThrow(() -> validator.validateIdentifier("props"));
    assertDoesNotThrow(() -> validator.validateIdentifier("myColumn"));
    assertDoesNotThrow(() -> validator.validateIdentifier("UPPERCASE"));
  }

  @Test
  void testValidIdentifierStartsWithUnderscore() {
    assertDoesNotThrow(() -> validator.validateIdentifier("_internal"));
    assertDoesNotThrow(() -> validator.validateIdentifier("_"));
  }

  @Test
  void testValidIdentifierWithNumbers() {
    assertDoesNotThrow(() -> validator.validateIdentifier("field123"));
    assertDoesNotThrow(() -> validator.validateIdentifier("col_1"));
  }

  @Test
  void testValidIdentifierWithHyphens() {
    assertDoesNotThrow(() -> validator.validateIdentifier("repo-url"));
  }

  @Test
  void testInvalidIdentifierNull() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier(null));
    assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  void testInvalidIdentifierEmpty() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier(""));
    assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  void testInvalidIdentifierStartsWithNumber() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("123column"));
    assertTrue(ex.getMessage().contains("Must start with a letter or underscore"));
  }

  @Test
  void testInvalidIdentifierSqlInjection_DropTable() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> validator.validateIdentifier("props\"; DROP TABLE users; --"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifierSqlInjection_Quote() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("column\"name"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifierSqlInjection_Semicolon() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("col;SELECT"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testValidIdentifierWithDotNotation() {
    assertDoesNotThrow(() -> validator.validateIdentifier("field.name"));
    assertDoesNotThrow(() -> validator.validateIdentifier("api.dataTypeIds"));
    assertDoesNotThrow(() -> validator.validateIdentifier("nested.field.name"));
    assertDoesNotThrow(() -> validator.validateIdentifier("internal.field"));
  }

  @Test
  void testInvalidDotNotation() {
    // Can't start with a dot
    SecurityException ex1 =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier(".field"));
    assertTrue(ex1.getMessage().contains("invalid"));

    // Can't end with a dot
    SecurityException ex2 =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("field."));
    assertTrue(ex2.getMessage().contains("invalid"));

    // Can't have consecutive dots
    SecurityException ex3 =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("field..name"));
    assertTrue(ex3.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifierSpace() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("field name"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testInvalidIdentifierTooLong() {
    String longIdentifier = "a".repeat(64); // PostgreSQL max is 63
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier(longIdentifier));
    assertTrue(ex.getMessage().contains("exceeds maximum length"));
  }

  @Test
  void testValidIdentifierMaxLength() {
    String maxLengthIdentifier = "a".repeat(63); // Exactly at limit
    assertDoesNotThrow(() -> validator.validateIdentifier(maxLengthIdentifier));
  }

  @Test
  void testValidJsonPathSingleLevel() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("brand")));
  }

  @Test
  void testValidJsonPathMultiLevel() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("seller", "address", "city")));
  }

  @Test
  void testValidJsonPathWithNumbers() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("field123")));
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("field_1")));
  }

  @Test
  void testInvalidJsonPathStartsWithNumber() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("1st_choice")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testValidJsonPathStartsWithUnderscore() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("_private")));
  }

  @Test
  void testInvalidJsonPathNull() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(null));
    assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  void testInvalidJsonPathEmpty() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(List.of()));
    assertTrue(ex.getMessage().contains("cannot be null or empty"));
  }

  @Test
  void testInvalidJsonPathNullElement() {
    List<String> pathWithNull = new ArrayList<>();
    pathWithNull.add("field");
    pathWithNull.add(null);
    pathWithNull.add("name");

    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(pathWithNull));
    assertTrue(ex.getMessage().contains("null or empty"));
  }

  @Test
  void testInvalidJsonPathEmptyElement() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> validator.validateJsonPath(List.of("field", "", "name")));
    assertTrue(ex.getMessage().contains("null or empty"));
  }

  @Test
  void testInvalidJsonPathSqlInjectionQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("brand' OR '1'='1")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPathSqlInjectionDoubleQuote() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("name\"--")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPathSqlInjectionSemicolon() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> validator.validateJsonPath(List.of("field; DROP TABLE")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testValidJsonPathWithHyphen() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("field-name")));
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("user-id")));
  }

  @Test
  void testValidJsonPathWithDotNotation() {
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("field.name")));
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of("user.address.city")));
  }

  @Test
  void testInvalidJsonPathSpace() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("field name")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testInvalidJsonPathTooDeep() {
    List<String> deepPath = new ArrayList<>();
    for (int i = 0; i < 11; i++) { // Max is 10
      deepPath.add("level" + i);
    }
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(deepPath));
    assertTrue(ex.getMessage().contains("exceeds maximum depth"));
  }

  @Test
  void testValidJsonPathMaxDepth() {
    List<String> deepPath = new ArrayList<>();
    for (int i = 0; i < 10; i++) { // Exactly at limit
      deepPath.add("level" + i);
    }
    assertDoesNotThrow(() -> validator.validateJsonPath(deepPath));
  }

  @Test
  void testInvalidJsonPathFieldTooLong() {
    String longField = "a".repeat(101); // Max is 100
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateJsonPath(List.of(longField)));
    assertTrue(ex.getMessage().contains("exceeds maximum length"));
  }

  @Test
  void testValidJsonPathMaxFieldLength() {
    String maxLengthField = "a".repeat(100); // Exactly at limit
    assertDoesNotThrow(() -> validator.validateJsonPath(List.of(maxLengthField)));
  }

  @Test
  void testAttackScenarioCommentInjection() {
    SecurityException ex =
        assertThrows(SecurityException.class, () -> validator.validateIdentifier("field\"--"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testAttackScenarioUnionInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateIdentifier("col\" UNION SELECT"));
    assertTrue(ex.getMessage().contains("invalid"));
  }

  @Test
  void testAttackScenarioOrInjection() {
    SecurityException ex =
        assertThrows(
            SecurityException.class,
            () -> validator.validateJsonPath(List.of("field\" OR \"1\"=\"1")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }

  @Test
  void testAttackScenarioNestedQuotes() {
    SecurityException ex =
        assertThrows(
            SecurityException.class, () -> validator.validateJsonPath(List.of("field'\"'; DROP")));
    assertTrue(ex.getMessage().contains("invalid characters"));
  }
}
