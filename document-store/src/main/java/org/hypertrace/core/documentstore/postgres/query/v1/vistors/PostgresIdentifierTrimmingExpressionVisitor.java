package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.getType;

import javax.annotation.Nullable;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

public class PostgresIdentifierTrimmingExpressionVisitor
    extends PostgresSelectTypeExpressionVisitor {

  @Nullable private final SelectTypeExpression rhs;

  public PostgresIdentifierTrimmingExpressionVisitor(
      final PostgresSelectTypeExpressionVisitor baseVisitor,
      @Nullable final SelectTypeExpression rhs) {
    super(baseVisitor);
    this.rhs = rhs;
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    final String parsed = expression.accept(baseVisitor);

    if (rhs == null) {
      return parsed;
    }

    final Type type = getType(rhs.accept(new PostgresConstantExpressionVisitor()));

    if (type == Type.STRING) {
      return String.format("TRIM('\"' FROM %s::text)", parsed);
    }

    return parsed;
  }
}
