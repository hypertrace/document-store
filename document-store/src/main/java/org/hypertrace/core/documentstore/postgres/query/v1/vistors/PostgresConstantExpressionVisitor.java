package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;

@NoArgsConstructor
public class PostgresConstantExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresConstantExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  @Override
  public Object visit(final ConstantExpression expression) {
    return expression.getValue();
  }
}
