package org.hypertrace.core.documentstore.expression;

import org.hypertrace.core.documentstore.parser.IGroupingExpressionParser;

/**
 * An interface to represent that the expression can be used in the
 * GROUP BY clause of the query.
 */
public interface GroupingExpression {
  void parse(IGroupingExpressionParser parser);
}
