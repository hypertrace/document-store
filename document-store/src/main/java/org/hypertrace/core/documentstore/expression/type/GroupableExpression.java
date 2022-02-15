package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.parser.GroupableExpressionVisitor;

/**
 * An interface to represent that the expression can be used in the GROUP BY clause of the query.
 */
public interface GroupableExpression {
  <T> T accept(final GroupableExpressionVisitor visitor);
}
