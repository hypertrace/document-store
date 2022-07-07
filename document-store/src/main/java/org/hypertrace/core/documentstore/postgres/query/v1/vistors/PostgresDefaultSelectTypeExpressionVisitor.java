package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PostgresDefaultSelectTypeExpressionVisitor
    extends PostgresSelectTypeExpressionVisitor {
  static final PostgresDefaultSelectTypeExpressionVisitor INSTANCE =
      new PostgresDefaultSelectTypeExpressionVisitor();

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
