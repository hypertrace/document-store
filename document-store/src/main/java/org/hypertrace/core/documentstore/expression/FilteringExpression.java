package org.hypertrace.core.documentstore.expression;

import org.hypertrace.core.documentstore.parser.FilteringExpressionParser;

/**
 * An interface to represent that the expression can be used in either the WHERE clause or the
 * HAVING clause of the query.
 */
public interface FilteringExpression {
  Object parse(FilteringExpressionParser parser);
}
