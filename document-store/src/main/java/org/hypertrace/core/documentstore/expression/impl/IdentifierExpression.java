package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.GroupableExpression;
import org.hypertrace.core.documentstore.expression.type.SelectableExpression;
import org.hypertrace.core.documentstore.expression.type.SortableExpression;
import org.hypertrace.core.documentstore.parser.GroupableExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectableExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortableExpressionVisitor;

/**
 * Expression representing either an identifier/column name
 *
 * <p>Example: IdentifierExpression.of("col1");
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class IdentifierExpression
    implements GroupableExpression, SelectableExpression, SortableExpression {

  String name;

  public static IdentifierExpression of(final String name) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    return new IdentifierExpression(name);
  }

  @Override
  public <T> T accept(final GroupableExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SelectableExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SortableExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
