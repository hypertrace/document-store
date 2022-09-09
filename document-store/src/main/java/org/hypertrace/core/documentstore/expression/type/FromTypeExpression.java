package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.model.Hashable;
import org.hypertrace.core.documentstore.model.Printable;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;

/**
 * Expression to retrieve rows from the referenced tables Implementations can perform table
 * functions, join, lateral subqueries
 */
public interface FromTypeExpression extends Hashable, Printable {
  <T> T accept(final FromTypeExpressionVisitor visitor);
}
