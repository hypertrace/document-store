package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.JoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;

public interface FromTypeExpressionVisitor {
  <T> T visit(final UnnestExpression unnestExpression);

  <T> T visit(final JoinExpression joinExpression);
}
