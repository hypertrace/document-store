package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

@NoArgsConstructor
public class PostgresIdentifierExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  @Setter private boolean isFieldAccessor = false;

  public PostgresIdentifierExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    return isFieldAccessor
        ? PostgresUtils.prepareFieldAccessorExpr(expression.getName()).toString()
        : expression.getName();
  }
}
