package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.expression.model.Hashable;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;

/**
 * An interface to represent that the expression can be used in the GROUP BY clause of the query.
 */
public interface GroupTypeExpression extends Hashable {
  <T> T accept(final GroupTypeExpressionVisitor visitor);
}
