package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
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
}
