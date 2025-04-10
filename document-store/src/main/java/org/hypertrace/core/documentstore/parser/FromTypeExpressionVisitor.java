package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;

public interface FromTypeExpressionVisitor {
  <T> T visit(UnnestExpression unnestExpression);

  <T> T visit(SubQueryJoinExpression subQueryJoinExpression);
}
