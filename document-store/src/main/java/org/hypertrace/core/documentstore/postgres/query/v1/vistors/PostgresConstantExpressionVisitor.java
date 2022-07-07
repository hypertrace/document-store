package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;

public class PostgresConstantExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  @Override
  public Object visit(final ConstantExpression expression) {
    return expression.getValue();
  }
}
