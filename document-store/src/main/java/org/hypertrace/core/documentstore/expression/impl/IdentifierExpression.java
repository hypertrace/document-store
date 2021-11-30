package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotBlank;
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

  @NotBlank String name;

  public static IdentifierExpression of(final String name) {
    return validateAndReturn(new IdentifierExpression(name));
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
