package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.getType;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

public class PostgresIdentifierTrimmingExpressionVisitor
    extends PostgresSelectTypeExpressionVisitor {

  private final Object value;

  public PostgresIdentifierTrimmingExpressionVisitor(
      final PostgresSelectTypeExpressionVisitor baseVisitor, final Object value) {
    super(baseVisitor);
    this.value = value;
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    final Type type = getType(value);
    final String parsed = expression.accept(baseVisitor);

    if (type == Type.STRING) {
      return String.format("TRIM('\"' FROM %s::text)", parsed);
    }

    return parsed;
  }
}
