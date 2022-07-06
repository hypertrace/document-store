package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

@NoArgsConstructor
public class PostgresFieldIdentifierExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresFieldIdentifierExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    return PostgresUtils.prepareFieldAccessorExpr(expression.getName()).toString();
  }
}
