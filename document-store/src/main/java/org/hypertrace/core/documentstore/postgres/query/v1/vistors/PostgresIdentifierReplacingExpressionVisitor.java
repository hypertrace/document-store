package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

public class PostgresIdentifierReplacingExpressionVisitor
    extends PostgresSelectTypeExpressionVisitor {

  private final String source;
  private final String target;

  public PostgresIdentifierReplacingExpressionVisitor(
      final PostgresSelectTypeExpressionVisitor baseVisitor,
      final String source,
      final String target) {
    super(baseVisitor);
    this.source = source;
    this.target = target;
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    final String parsed = expression.accept(baseVisitor);
    return parsed.equals(source) ? target : source;
  }
}
