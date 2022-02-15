package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.parser.FilterableExpressionVisitor;

/**
 * An interface to represent that the expression can be used in either the WHERE clause or the
 * HAVING clause of the query.
 */
public interface FilterableExpression {
  <T> T accept(final FilterableExpressionVisitor visitor);
}
