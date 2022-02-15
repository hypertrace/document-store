package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public interface GroupTypeExpressionVisitor {
  <T> T visit(final FunctionExpression expression);

  <T> T visit(final IdentifierExpression expression);
}
