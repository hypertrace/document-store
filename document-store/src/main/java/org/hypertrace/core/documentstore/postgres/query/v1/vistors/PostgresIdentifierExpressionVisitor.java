package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public class PostgresIdentifierExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  @Override
  public String visit(final IdentifierExpression expression) {
    return expression.getName();
  }
}
