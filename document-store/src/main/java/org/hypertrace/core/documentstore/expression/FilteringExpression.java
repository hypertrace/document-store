package org.hypertrace.core.documentstore.expression;

import org.hypertrace.core.documentstore.parser.IFilteringExpressionParser;

/**
 * A marker interface to represent that the expression is filterable and can be used in either the
 * WHERE clause or the HAVING clause of the query.
 */
public interface FilteringExpression {
  void parse(IFilteringExpressionParser parser);
}
