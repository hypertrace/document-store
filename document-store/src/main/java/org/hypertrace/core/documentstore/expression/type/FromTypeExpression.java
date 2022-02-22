package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;

/** Interface representing expression used to transform the source table */
public interface FromTypeExpression {
  <T> T accept(final FromTypeExpressionVisitor visitor);
}
