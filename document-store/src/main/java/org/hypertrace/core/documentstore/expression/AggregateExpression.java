package org.hypertrace.core.documentstore.expression;

import lombok.Value;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;
import org.hypertrace.core.documentstore.parser.SortingExpressionParser;

/**
 * Expression representing aggregation in a query.
 *
 * <p>Example: SUM(col1) can be constructed as <code>
 *    AggregatorExpression.of(Aggregator.SUM, LiteralExpression.of("col1"));
 * </code>
 */
@Value(staticConstructor = "of")
public class AggregateExpression implements SelectingExpression, SortingExpression {
  AggregationOperator aggregator;
  SelectingExpression expression;

  @Override
  public Object parse(SelectingExpressionParser parser) {
    return parser.parse(this);
  }

  @Override
  public Object parse(SortingExpressionParser parser) {
    return parser.parse(this);
  }
}
