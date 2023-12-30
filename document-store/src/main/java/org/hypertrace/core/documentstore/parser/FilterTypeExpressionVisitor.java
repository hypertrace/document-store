package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

public interface FilterTypeExpressionVisitor {
  <T> T visit(final LogicalExpression expression);

  <T> T visit(final RelationalExpression expression);

  <T> T visit(final KeyExpression expression);

  <T> T visit(final ArrayRelationalFilterExpression expression);

  <T> T visit(final DocumentArrayFilterExpression expression);
}
