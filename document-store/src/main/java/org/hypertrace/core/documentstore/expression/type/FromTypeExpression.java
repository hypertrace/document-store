package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;

public interface FromTypeExpression {
  <T> T accept(final FromTypeExpressionVisitor visitor);
}
