package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;

public interface FromTypeExpressionVisitor {
  <T> T visit(UnnestExpression unnestExpression);

  /*
   * Subquery join expression is not supported by default. Override this method to support it.
   */
  default <T> T visit(SubQueryJoinExpression subQueryJoinExpression) {
    throw new UnsupportedOperationException("This operation is not supported");
  }
}
