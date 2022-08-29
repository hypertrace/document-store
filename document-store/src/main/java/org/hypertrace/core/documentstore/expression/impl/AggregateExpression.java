package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SortTypeExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;

/**
 * Expression representing aggregation in a query.
 *
 * <p>Example: SUM(col1) can be constructed as <code>
 * AggregatorExpression.of(AggregationOperator.SUM, IdentifierExpression.of("col1"));
 * </code>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AggregateExpression implements SelectTypeExpression, SortTypeExpression {

  AggregationOperator aggregator;

  SelectTypeExpression expression;

  public static AggregateExpression of(
      final AggregationOperator aggregator, final SelectTypeExpression expression) {
    Preconditions.checkArgument(aggregator != null, "aggregator is null");
    Preconditions.checkArgument(expression != null, "expression is null");
    return new AggregateExpression(aggregator, expression);
  }

  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SortTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return aggregator + "(" + expression + ")";
  }
}
