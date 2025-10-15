package org.hypertrace.core.documentstore.expression.impl;

import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;

/**
 * Expression representing a nested field within a JSONB column in flat Postgres collections.
 *
 * <p>Example:
 *
 * <pre>{@code
 * JsonIdentifierExpression expr = JsonIdentifierExpression.builder()
 *     .columnName("customAttr")
 *     .jsonPath(List.of("myAttribute"))
 *     .jsonType("STRING")
 *     .build();
 * }</pre>
 *
 * <p>This generates SQL like: {@code customAttr -> 'myAttribute' ->> (cast as text)}
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class JsonIdentifierExpression extends IdentifierExpression {

  private final String columnName; // e.g., "customAttr" (the actual JSONB column)
  private final List<String> jsonPath; // e.g., ["myAttribute"] (path within the JSONB)
  private final String jsonType; // "STRING" or "STRING_ARRAY"

  @Builder
  private JsonIdentifierExpression(String columnName, List<String> jsonPath, String jsonType) {
    super(columnName + "." + String.join(".", jsonPath));
    this.columnName = columnName;
    this.jsonPath = jsonPath;
    this.jsonType = jsonType;
  }

  @Override
  public <T> T accept(GroupTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(SortTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return String.format(
        "JsonIdentifier{name=`%s`, column=`%s`, path=%s, type=%s}",
        getName(), columnName, jsonPath, jsonType);
  }
}
