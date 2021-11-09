package org.hypertrace.core.documentstore.expression;

import org.hypertrace.core.documentstore.parser.ISortingExpressionParser;

/**
 * An interface to represent that the expression can be used in the ORDER BY clause of the query.
 */
public interface SortingExpression {
  Object parse(ISortingExpressionParser parser);
}
