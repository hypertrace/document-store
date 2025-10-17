package org.hypertrace.core.documentstore.postgres.utils;

import java.util.List;

/** Validates PostgreSQL identifiers to prevent SQL injection attacks. */
public interface PostgresSecurityValidator {

  /**
   * Validates a PostgreSQL column or table name.
   *
   * @param identifier the column or table name to validate
   * @throws SecurityException if the identifier is invalid or potentially malicious
   */
  void validateIdentifier(String identifier);

  /**
   * Validates a JSON field path within a JSONB column.
   *
   * @param path the list of JSON field names forming the path (e.g., ["seller", "address", "city"])
   * @throws SecurityException if any field in the path is invalid or the path is too deep
   */
  void validateJsonPath(List<String> path);
}
