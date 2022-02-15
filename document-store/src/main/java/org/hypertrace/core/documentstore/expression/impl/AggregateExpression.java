package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.type.SelectableExpression;
import org.hypertrace.core.documentstore.expression.type.SortableExpression;
import org.hypertrace.core.documentstore.parser.SelectableExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortableExpressionVisitor;

/**
 * Expression representing aggregation in a query.
 *
 * <p>Example: SUM(col1) can be constructed as <code>
 * AggregatorExpression.of(AggregationOperator.SUM, IdentifierExpression.of("col1"));
 * </code>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AggregateExpression implements SelectableExpression, SortableExpression {

  AggregationOperator aggregator;

  SelectableExpression expression;

  public static AggregateExpression of(
      final AggregationOperator aggregator, final SelectableExpression expression) {
    Preconditions.checkArgument(aggregator != null, "aggregator is null");
    Preconditions.checkArgument(expression != null, "expression is null");
    return new AggregateExpression(aggregator, expression);
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
