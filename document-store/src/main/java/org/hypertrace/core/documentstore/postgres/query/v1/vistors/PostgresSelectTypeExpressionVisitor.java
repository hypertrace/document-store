package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

public abstract class PostgresSelectTypeExpressionVisitor implements SelectTypeExpressionVisitor {

  @Override
  public <T> T visit(final AggregateExpression expression) {
    throw new UnsupportedOperationException(String.valueOf(expression));
  }

  @Override
  public <T> T visit(final ConstantExpression expression) {
    throw new UnsupportedOperationException(String.valueOf(expression));
  }

  @Override
  public <T> T visit(final FunctionExpression expression) {
    throw new UnsupportedOperationException(String.valueOf(expression));
  }

  @Override
  public <T> T visit(final IdentifierExpression expression) {
    throw new UnsupportedOperationException(String.valueOf(expression));
  }
}
