package org.hypertrace.core.documentstore.expression.impl;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;

/**
 * Expression representing a nested field within a JSONB column in flat Postgres collections.
 *
 * <p>Example: JsonIdentifierExpression.of("customAttr", List.of("myAttribute"), "STRING");
 *
 * <p>This generates SQL like: customAttr -> 'myAttribute' ->> (cast as text)
 */
@Value
@EqualsAndHashCode(callSuper = true)
public class JsonIdentifierExpression extends IdentifierExpression {

  String columnName; // e.g., "customAttr" (the actual JSONB column)
  List<String> jsonPath; // e.g., ["myAttribute"] (path within the JSONB)
  String jsonType; // "STRING" or "STRING_ARRAY"

  public static JsonIdentifierExpression of(
      final String columnName, final List<String> jsonPath, final String jsonType) {
    // Construct full name for compatibility: "customAttr.myAttribute"
    String fullName = columnName + "." + String.join(".", jsonPath);
    return new JsonIdentifierExpression(fullName, columnName, jsonPath, jsonType);
  }

  private JsonIdentifierExpression(
      String name, String columnName, List<String> jsonPath, String jsonType) {
    super(name);
    this.columnName = columnName;
    this.jsonPath = jsonPath;
    this.jsonType = jsonType;
  }

  @Override
  public String toString() {
    return String.format(
        "JsonIdentifier{name=`%s`, column=`%s`, path=%s, type=%s}",
        getName(), columnName, jsonPath, jsonType);
  }
}
