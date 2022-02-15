package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.parser.SortableExpressionVisitor;

/**
 * An interface to represent that the expression can be used in the ORDER BY clause of the query.
 */
public interface SortableExpression {
  <T> T accept(final SortableExpressionVisitor visitor);
}
