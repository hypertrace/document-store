package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.CacheStrategy;
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
@EqualsAndHashCode(cacheStrategy = CacheStrategy.LAZY)
public class AggregateExpression implements SelectingExpression, SortingExpression {

  @NotNull AggregationOperator aggregator;

  @NotNull SelectingExpression expression;

  public static AggregateExpression of(
      final AggregationOperator aggregator, final SelectingExpression expression) {
    return validateAndReturn(new AggregateExpression(aggregator, expression));
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
