package org.hypertrace.core.documentstore.postgres.utils;

import java.util.List;

/**
 * Validates PostgreSQL identifiers to prevent SQL injection attacks.
 *
 * <p>This validator is schema-agnostic and uses pattern matching rather than whitelisting to
 * support dynamic schemas.
 *
 * <p>Implementations should enforce different rules for different identifier types:
 *
 * <ul>
 *   <li><b>Column/Table Names:</b> Must follow PostgreSQL identifier rules (start with letter or
 *       underscore)
 *   <li><b>JSON Field Names:</b> More permissive (can start with numbers for flexibility)
 * </ul>
 *
 * <p><b>References:</b>
 *
 * <ul>
 *   <li>PostgreSQL Docs: <a
 *       href="https://www.postgresql.org/docs/current/sql-syntax-lexical.html">SQL Syntax:
 *       Identifiers</a>
 *   <li>OWASP SQL Injection: <a
 *       href="https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html">Prevention
 *       Cheat Sheet</a>
 *   <li>CWE-89: <a href="https://cwe.mitre.org/data/definitions/89.html">SQL Injection</a>
 * </ul>
 */
public interface PostgresSecurityValidator {

  /**
   * Validates a PostgreSQL column or table name.
   *
   * <p>Implementations should enforce PostgreSQL identifier rules:
   *
   * <ul>
   *   <li>Must start with a letter (a-z, A-Z) or underscore (_)
   *   <li>Can contain letters, digits (0-9), and underscores
   *   <li>Maximum length: typically 63 characters (PostgreSQL limit)
   * </ul>
   *
   * @param identifier the column or table name to validate
   * @throws SecurityException if the identifier is invalid or potentially malicious
   */
  void validateIdentifier(String identifier);

  /**
   * Validates a JSON field path within a JSONB column.
   *
   * <p>Implementations should enforce rules for JSON field names:
   *
   * <ul>
   *   <li>Can start with a letter, digit, or underscore
   *   <li>Can contain only letters, digits, and underscores
   *   <li>Maximum length per field: reasonable limit (e.g., 100 characters)
   *   <li>Maximum path depth: reasonable limit (e.g., 10 levels)
   * </ul>
   *
   * @param path the list of JSON field names forming the path (e.g., ["seller", "address", "city"])
   * @throws SecurityException if any field in the path is invalid or the path is too deep
   */
  void validateJsonPath(List<String> path);
}
