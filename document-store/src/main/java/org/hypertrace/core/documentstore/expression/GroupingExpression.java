package org.hypertrace.core.documentstore.expression;

import org.hypertrace.core.documentstore.parser.IGroupingExpressionParser;

/**
 * A marker interface to represent that the expression is groupable and can be used in either the
 * GROUP BY clause of the query.
 */
public interface GroupingExpression {
  void parse(IGroupingExpressionParser parser);
}
