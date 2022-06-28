package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.expression.model.Hashable;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

/** An interface to represent that the expression can be used in the SELECT clause of the query. */
public interface SelectTypeExpression extends Hashable {
  <T> T accept(final SelectTypeExpressionVisitor visitor);
}
