package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@NoArgsConstructor
public class PostgresIdentifierExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresIdentifierExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  public PostgresIdentifierExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    return expression.getName();
  }

  @Override
  public String visit(final JsonIdentifierExpression expression) {
    return expression.getName();
  }
}
