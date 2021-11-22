package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

public interface FilteringExpressionVisitor {
  <T> T visit(final LogicalExpression expression);

  <T> T visit(final RelationalExpression expression);
}
