package org.hypertrace.core.documentstore.expression;

import lombok.Value;
import org.hypertrace.core.documentstore.parser.ISelectingExpressionParser;
import org.hypertrace.core.documentstore.parser.ISortingExpressionParser;

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
  public Object parse(ISelectingExpressionParser parser) {
    return parser.parse(this);
  }

  @Override
  public Object parse(ISortingExpressionParser parser) {
    return parser.parse(this);
  }
}
