package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.mapper.FieldNameToColumnMapper;
import org.hypertrace.core.documentstore.postgres.query.v1.mapper.FieldNameToColumnMapper.PgFieldColumn;
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
    PgFieldColumn pgFieldColumn =
        FieldNameToColumnMapper.toColumnName(getPostgresQueryParser(), expression.getName());
    return PostgresUtils.prepareFieldAccessorExpr(
            pgFieldColumn.getFieldName(), pgFieldColumn.getColumnName())
        .toString();
  }
}
