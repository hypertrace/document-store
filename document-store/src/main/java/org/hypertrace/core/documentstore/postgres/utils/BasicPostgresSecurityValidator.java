package org.hypertrace.core.documentstore.postgres.utils;

import java.util.List;
import java.util.regex.Pattern;

/** Basic implementation of PostgresSecurityValidator using pattern matching. */
public class BasicPostgresSecurityValidator implements PostgresSecurityValidator {

  // Default values
  private static final int DEFAULT_MAX_IDENTIFIER_LENGTH = 63;
  private static final int DEFAULT_MAX_JSON_FIELD_LENGTH = 100;
  private static final int DEFAULT_MAX_JSON_PATH_DEPTH = 10;

  /**
   * Default pattern for PostgreSQL column/table identifiers.
   *
   * <p>Pattern: {@code ^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$}
   *
   * <p><b>Allowed:</b>
   *
   * <ul>
   *   <li>Must start with: letter (a-z, A-Z) or underscore (_)
   *   <li>Can contain: letters (a-z, A-Z), digits (0-9), underscores (_), dots (.) for nested field
   *       notation
   *   <li>Examples: {@code "myColumn"}, {@code "user_id"}, {@code "_internal"}, {@code "TABLE1"},
   *       {@code "field.name"}, {@code "nested.field.name"}
   * </ul>
   *
   * <p><b>Not allowed:</b>
   *
   * <ul>
   *   <li>Starting with numbers: {@code "123column"}
   *   <li>Starting or ending with dots: {@code ".field"}, {@code "field."}
   *   <li>Consecutive dots: {@code "field..name"}
   *   <li>Special characters: {@code "col-name"}
   *   <li>Spaces: {@code "my column"}, {@code "field OR 1=1"}
   *   <li>Quotes: {@code "field\"name"}, {@code "field'name"}
   *   <li>Semicolons: {@code "col;DROP"}
   * </ul>
   *
   * <p>Follows PostgreSQL identifier rules as defined in: <a
   * href="https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS">PostgreSQL
   * Documentation</a>
   */
  private static final String DEFAULT_IDENTIFIER_PATTERN =
      "^[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*$";

  /**
   * Default pattern for JSON field names within JSONB columns.
   *
   * <p>Pattern: {@code ^[a-zA-Z0-9_]+$}
   *
   * <p><b>Allowed:</b>
   *
   * <ul>
   *   <li>Can start with: letter (a-z, A-Z), digit (0-9), or underscore (_)
   *   <li>Can contain: letters (a-z, A-Z), digits (0-9), underscores (_)
   *   <li>Examples: {@code "brand"}, {@code "1st_choice"}, {@code "field123"}, {@code "_private"}
   * </ul>
   *
   * <p><b>Not allowed:</b>
   *
   * <ul>
   *   <li>Hyphens: {@code "field-name"}
   *   <li>Dots: {@code "field.name"} (use path segments instead: ["field", "name"])
   *   <li>Spaces: {@code "my field"}
   *   <li>Special characters: {@code "field@name"}, {@code "field#name"}
   *   <li>Quotes: {@code "field\"name"}, {@code "field'name"}
   *   <li>Semicolons: {@code "field; DROP TABLE"}
   *   <li>SQL operators: {@code "field\" OR \"1\"=\"1"}
   * </ul>
   *
   * <p><b>Note:</b> More permissive than identifier pattern to support flexible JSON schemas where
   * field names may start with numbers.
   */
  private static final String DEFAULT_JSON_FIELD_PATTERN = "^[a-zA-Z0-9_]+$";

  /** Default instance with hardcoded values for convenient static access. */
  private static final BasicPostgresSecurityValidator DEFAULT =
      new BasicPostgresSecurityValidator(
          DEFAULT_MAX_IDENTIFIER_LENGTH,
          DEFAULT_MAX_JSON_FIELD_LENGTH,
          DEFAULT_MAX_JSON_PATH_DEPTH,
          DEFAULT_IDENTIFIER_PATTERN,
          DEFAULT_JSON_FIELD_PATTERN);

  // Instance variables for configured limits
  private final Pattern validIdentifier;
  private final Pattern validJsonField;
  private final int maxIdentifierLength;
  private final int maxJsonFieldLength;
  private final int maxJsonPathDepth;

  /**
   * Returns the default validator instance with hardcoded values.
   *
   * @return the default validator instance
   */
  public static BasicPostgresSecurityValidator getDefault() {
    return DEFAULT;
  }

  /** Private constructor - use {@link #getDefault()} to get the validator instance. */
  private BasicPostgresSecurityValidator(
      int maxIdentifierLength,
      int maxJsonFieldLength,
      int maxJsonPathDepth,
      String identifierPattern,
      String jsonFieldPattern) {
    this.maxIdentifierLength = maxIdentifierLength;
    this.maxJsonFieldLength = maxJsonFieldLength;
    this.maxJsonPathDepth = maxJsonPathDepth;
    this.validIdentifier = Pattern.compile(identifierPattern);
    this.validJsonField = Pattern.compile(jsonFieldPattern);
  }

  @Override
  public void validateIdentifier(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      throw new SecurityException("Identifier cannot be null or empty");
    }

    if (identifier.length() > maxIdentifierLength) {
      throw new SecurityException(
          String.format(
              "Identifier '%s' exceeds maximum length of %d characters",
              identifier, maxIdentifierLength));
    }

    if (!validIdentifier.matcher(identifier).matches()) {
      throw new SecurityException(
          String.format(
              "Identifier '%s' is invalid. Must start with a letter or underscore, "
                  + "and contain only letters, numbers, underscores, and dots (for proper dot notation).",
              identifier));
    }
  }

  @Override
  public void validateJsonPath(List<String> path) {
    if (path == null || path.isEmpty()) {
      throw new SecurityException("JSON path cannot be null or empty");
    }

    if (path.size() > maxJsonPathDepth) {
      throw new SecurityException(
          String.format(
              "JSON path depth %d exceeds maximum depth of %d", path.size(), maxJsonPathDepth));
    }

    for (int i = 0; i < path.size(); i++) {
      String field = path.get(i);

      if (field == null || field.isEmpty()) {
        throw new SecurityException(
            String.format("JSON path element at index %d is null or empty", i));
      }

      if (field.length() > maxJsonFieldLength) {
        throw new SecurityException(
            String.format(
                "JSON field '%s' at index %d exceeds maximum length of %d characters",
                field, i, maxJsonFieldLength));
      }

      if (!validJsonField.matcher(field).matches()) {
        throw new SecurityException(
            String.format(
                "JSON field '%s' at index %d contains invalid characters. "
                    + "Only alphanumeric characters and underscores are allowed.",
                field, i));
      }
    }
  }
}
