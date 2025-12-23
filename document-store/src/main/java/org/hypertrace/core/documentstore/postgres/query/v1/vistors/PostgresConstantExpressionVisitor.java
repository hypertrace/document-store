package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DateConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@NoArgsConstructor
public class PostgresConstantExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresConstantExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  public PostgresConstantExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @Override
  public Object visit(final ConstantExpression expression) {
    return expression.getValue();
  }

  @Override
  public Object visit(final DocumentConstantExpression expression) {
    return expression.getValue();
  }

  @Override
  public Object visit(final DateConstantExpression expression) {
    throw new UnsupportedOperationException("DateConstantExpression is not supported for Postgres");
  }
}
