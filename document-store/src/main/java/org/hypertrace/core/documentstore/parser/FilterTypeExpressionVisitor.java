package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.LogicExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationExpression;

public interface FilterTypeExpressionVisitor {
  <T> T visit(final LogicExpression expression);

  <T> T visit(final RelationExpression expression);
}
