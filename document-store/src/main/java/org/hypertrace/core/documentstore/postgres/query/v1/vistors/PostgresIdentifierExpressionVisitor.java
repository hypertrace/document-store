package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

@NoArgsConstructor
public class PostgresIdentifierExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresIdentifierExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    return expression.getName();
  }
}
