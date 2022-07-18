package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FieldToPgColumn;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

@NoArgsConstructor
public class PostgresFieldIdentifierExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresFieldIdentifierExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  public PostgresFieldIdentifierExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    FieldToPgColumn fieldToPgColumn =
        getPostgresQueryParser().getToPgColumnTransformer().transform(expression.getName());
    return PostgresUtils.prepareFieldAccessorExpr(
            fieldToPgColumn.getTransformedField(), fieldToPgColumn.getPgColumn())
        .toString();
  }
}
