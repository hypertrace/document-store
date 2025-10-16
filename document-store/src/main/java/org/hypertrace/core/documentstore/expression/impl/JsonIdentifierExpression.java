package org.hypertrace.core.documentstore.expression.impl;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

/**
 * Expression representing a nested field within a JSONB column in flat Postgres collections.
 *
 * <p>Example: JsonIdentifierExpression.of("customAttr", List.of("myAttribute"));
 *
 * <p>This generates SQL like: customAttr -> 'myAttribute' (returns JSON)
 *
 * <p><strong>Security Note:</strong> Column names and JSON paths are validated to prevent SQL
 * injection attacks, as they are not parameterized in PreparedStatements.
 */
@Value
@EqualsAndHashCode(callSuper = true)
public class JsonIdentifierExpression extends IdentifierExpression {

  String columnName; // e.g., "customAttr" (the actual JSONB column)
  List<String> jsonPath; // e.g., ["myAttribute"] (path within the JSONB)

  public static JsonIdentifierExpression of(
      final String columnName, final List<String> jsonPath) {
    // Validate column name to prevent SQL injection
    PostgresUtils.validateIdentifier(columnName, "Column name");

    // Validate each element in the JSON path
    if (jsonPath == null || jsonPath.isEmpty()) {
      throw new IllegalArgumentException("JSON path cannot be null or empty");
    }
    for (int i = 0; i < jsonPath.size(); i++) {
      PostgresUtils.validateIdentifier(
          jsonPath.get(i), "JSON path element at index " + i);
    }

    // Construct full name for compatibility: "customAttr.myAttribute"
    String fullName = columnName + "." + String.join(".", jsonPath);
    return new JsonIdentifierExpression(fullName, columnName, jsonPath);
  }

  private JsonIdentifierExpression(
      String name, String columnName, List<String> jsonPath) {
    super(name);
    this.columnName = columnName;
    this.jsonPath = jsonPath;
  }

  @Override
  public String toString() {
    return String.format(
        "JsonIdentifier{name=`%s`, column=`%s`, path=%s}",
        getName(), columnName, jsonPath);
  }
}
