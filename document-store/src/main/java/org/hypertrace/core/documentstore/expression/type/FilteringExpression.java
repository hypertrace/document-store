package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.parser.FilteringExpressionVisitor;

/**
 * An interface to represent that the expression can be used in either the WHERE clause or the
 * HAVING clause of the query.
 */
public interface FilteringExpression {
  <T> T visit(final FilteringExpressionVisitor visitor);
}
