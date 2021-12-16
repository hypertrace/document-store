package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;
import org.hypertrace.core.documentstore.parser.GroupingExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortingExpressionVisitor;

/**
 * Expression representing either an identifier/column name
 *
 * <p>Example: IdentifierExpression.of("col1");
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class IdentifierExpression
    implements GroupingExpression, SelectingExpression, SortingExpression {

  String name;

  public static IdentifierExpression of(final String name) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    return new IdentifierExpression(name);
  }

  @Override
  public <T> T visit(final GroupingExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T visit(final SelectingExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T visit(final SortingExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
