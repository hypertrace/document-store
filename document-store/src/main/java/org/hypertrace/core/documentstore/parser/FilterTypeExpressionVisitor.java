package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.ArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

public interface FilterTypeExpressionVisitor {
  <T> T visit(final LogicalExpression expression);

  <T> T visit(final RelationalExpression expression);

  <T> T visit(final KeyExpression expression);

  <T> T visit(final ArrayFilterExpression expression);
}
