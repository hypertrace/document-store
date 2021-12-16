package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortingExpressionVisitor;

/**
 * Expression representing aggregation in a query.
 *
 * <p>Example: SUM(col1) can be constructed as <code>
 * AggregatorExpression.of(AggregationOperator.SUM, IdentifierExpression.of("col1"));
 * </code>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AggregateExpression implements SelectingExpression, SortingExpression {

  AggregationOperator aggregator;

  SelectingExpression expression;

  public static AggregateExpression of(
      final AggregationOperator aggregator, final SelectingExpression expression) {
    Preconditions.checkArgument(aggregator != null, "expression is null");
    Preconditions.checkArgument(expression != null, "expression is null");
    return new AggregateExpression(aggregator, expression);
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
