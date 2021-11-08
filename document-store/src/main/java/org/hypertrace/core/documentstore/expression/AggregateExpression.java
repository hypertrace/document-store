package org.hypertrace.core.documentstore.expression;

import lombok.Value;

/**
 * Expression representing aggregation in a query.
 *
 * Example:
 *   SUM(col1) can be constructed as
 *   AggregatorExpression.of(Aggregator.SUM, LiteralExpression.of("col1"));
 *
 */
@Value(staticConstructor = "of")
public class AggregateExpression implements Projectable, Sortable {
  Aggregator aggregator;
  Projectable expression;
}
