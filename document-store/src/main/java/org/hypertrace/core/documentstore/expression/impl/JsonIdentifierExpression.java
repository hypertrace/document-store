package org.hypertrace.core.documentstore.expression.impl;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.hypertrace.core.documentstore.parser.FieldTransformationVisitor;
import org.hypertrace.core.documentstore.postgres.utils.BasicPostgresSecurityValidator;

/**
 * Expression representing a nested field within a JSONB column in flat Postgres collections.
 *
 * <p>Example: JsonIdentifierExpression.of("customAttr", "myAttribute", "nestedField");
 *
 * <p>This generates SQL like: customAttr -> 'myAttribute' -> 'nestedField' (returns JSON)
 */
@Value
@EqualsAndHashCode(callSuper = true)
public class JsonIdentifierExpression extends IdentifierExpression {

  String columnName; // e.g., "customAttr" (the top-level JSONB column)
  List<String> jsonPath; // e.g., ["myAttribute", "nestedField"]

  public static JsonIdentifierExpression of(final String columnName, final String... pathElements) {
    if (pathElements == null || pathElements.length == 0) {
      // In this case, use IdentifierExpression
      throw new IllegalArgumentException("JSON path cannot be null or empty");
    }
    return of(columnName, List.of(pathElements));
  }

  public static JsonIdentifierExpression of(final String columnName, final List<String> jsonPath) {
    // Validate column name to prevent SQL injection
    BasicPostgresSecurityValidator.getDefault().validateIdentifier(columnName);

    // Validate each element in the JSON path
    if (jsonPath == null || jsonPath.isEmpty()) {
      throw new IllegalArgumentException("JSON path cannot be null or empty");
    }

    // Validate JSON path to prevent SQL injection
    BasicPostgresSecurityValidator.getDefault().validateJsonPath(jsonPath);

    // Create unmodifiable defensive copy using List.copyOf (Java 10+)
    // If already unmodifiable, returns the same instance
    List<String> unmodifiablePath = List.copyOf(jsonPath);

    // Construct full name for compatibility: "customAttr.myAttribute"
    String fullName = columnName + "." + String.join(".", unmodifiablePath);
    return new JsonIdentifierExpression(fullName, columnName, unmodifiablePath);
  }

  private JsonIdentifierExpression(String name, String columnName, List<String> jsonPath) {
    super(name);
    this.columnName = columnName;
    this.jsonPath = jsonPath;
  }

  /**
   * Accepts a field transformation visitor. Overrides the parent to dispatch to the
   * JsonIdentifierExpression-specific visit method.
   *
   * @param visitor The field transformation visitor
   * @param <T> The return type of the transformation
   * @return The transformed field representation
   */
  @Override
  public <T> T accept(final FieldTransformationVisitor<T> visitor) {
    return visitor.visit(this);
  }

  /**
   * Accepts a select type expression visitor. Overrides the parent to dispatch to the
   * JsonIdentifierExpression-specific visit method.
   *
   * @param visitor The select type expression visitor
   * @param <T> The return type
   * @return The result of visiting this expression
   */
  @Override
  public <T> T accept(final org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return String.format(
        "JsonIdentifier{name=`%s`, column=`%s`, path=%s}", getName(), columnName, jsonPath);
  }
}
