package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

@NoArgsConstructor
public class PostgresDataAccessorIdentifierExpressionVisitor
    extends PostgresSelectTypeExpressionVisitor {

  private Type type = Type.NUMERIC;

  public PostgresDataAccessorIdentifierExpressionVisitor(Type type) {
    this.type = type;
  }

  public PostgresDataAccessorIdentifierExpressionVisitor(
      PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  public PostgresDataAccessorIdentifierExpressionVisitor(
      PostgresSelectTypeExpressionVisitor baseVisitor, Type type) {
    super(baseVisitor);
    this.type = type;
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    String dataAccessor = PostgresUtils.prepareFieldDataAccessorExpr(expression.getName());
    if (type.equals(Type.NUMERIC)) {
      return PostgresUtils.prepareCast(dataAccessor, 1);
    } else if (type.equals(Type.BOOLEAN)) {
      return PostgresUtils.prepareCast(dataAccessor, true);
    }
    return PostgresUtils.prepareCast(dataAccessor, "");
  }
}
