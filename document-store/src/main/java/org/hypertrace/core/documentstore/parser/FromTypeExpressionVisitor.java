package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.JoinExpression;
import org.hypertrace.core.documentstore.expression.impl.SubQueryFromExpression;
import org.hypertrace.core.documentstore.expression.impl.TableFromExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;

public interface FromTypeExpressionVisitor {
  <T> T visit(UnnestExpression unnestExpression);

  <T> T visit(JoinExpression joinExpression);

  <T> T visit(TableFromExpression tableFromExpression);

  <T> T visit(SubQueryFromExpression subQueryFromExpression);
}
