package org.hypertrace.core.documentstore.expression.impl;

import java.util.List;
import lombok.EqualsAndHashCode;
import org.hypertrace.core.documentstore.postgres.utils.BasicPostgresSecurityValidator;

/**
 * Represents an identifier expression for array-typed fields inside JSONB columns. This allows
 * parsers to apply array-specific logic (e.g., jsonb_array_length checks for EXISTS operators to
 * exclude empty arrays).
 *
 * <p>Example: For a JSONB column "attributes" with a nested array field "tags":
 *
 * <pre>{"attributes": {"tags": ["value1", "value2"]}}</pre>
 *
 * Use: {@code JsonArrayIdentifierExpression.of("attributes", "tags")}
 */
@EqualsAndHashCode(callSuper = true)
public class JsonArrayIdentifierExpression extends JsonIdentifierExpression {

  public static JsonArrayIdentifierExpression of(
      final String columnName, final String... pathElements) {
    if (pathElements == null || pathElements.length == 0) {
      throw new IllegalArgumentException("JSON path cannot be null or empty for array field");
    }
    return of(columnName, List.of(pathElements));
  }

  public static JsonArrayIdentifierExpression of(
      final String columnName, final List<String> jsonPath) {
    // Validate inputs
    BasicPostgresSecurityValidator.getDefault().validateIdentifier(columnName);

    if (jsonPath == null || jsonPath.isEmpty()) {
      throw new IllegalArgumentException("JSON path cannot be null or empty for array field");
    }

    BasicPostgresSecurityValidator.getDefault().validateJsonPath(jsonPath);

    List<String> unmodifiablePath = List.copyOf(jsonPath);

    // Construct full name for compatibility: "customAttr.myAttribute"
    String fullName = columnName + "." + String.join(".", unmodifiablePath);
    return new JsonArrayIdentifierExpression(fullName, columnName, unmodifiablePath);
  }

  private JsonArrayIdentifierExpression(String name, String columnName, List<String> jsonPath) {
    super(name, columnName, jsonPath);
  }
}
