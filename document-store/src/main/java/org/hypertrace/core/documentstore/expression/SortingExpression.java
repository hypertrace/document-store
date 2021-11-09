package org.hypertrace.core.documentstore.expression;

import org.hypertrace.core.documentstore.parser.ISortingExpressionParser;

/**
 * A marker interface to represent that the expression is sortable and can be used in either the
 * ORDER BY clause of the query.
 */
public interface SortingExpression {
  void parse(ISortingExpressionParser parser);
}
