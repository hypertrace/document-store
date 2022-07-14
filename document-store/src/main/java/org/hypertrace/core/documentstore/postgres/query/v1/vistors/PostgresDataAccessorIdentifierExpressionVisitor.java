package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.mapper.FieldNameToColumnMapper;
import org.hypertrace.core.documentstore.postgres.query.v1.mapper.FieldNameToColumnMapper.PgFieldColumn;
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

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  public PostgresDataAccessorIdentifierExpressionVisitor(
      PostgresSelectTypeExpressionVisitor baseVisitor, Type type) {
    super(baseVisitor);
    this.type = type;
  }

  public PostgresDataAccessorIdentifierExpressionVisitor(
      PostgresQueryParser postgresQueryParser, Type type) {
    super(postgresQueryParser);
    this.type = type;
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    PgFieldColumn pgFieldColumn =
        FieldNameToColumnMapper.toColumnName(getPostgresQueryParser(), expression.getName());
    String dataAccessor =
        PostgresUtils.prepareFieldDataAccessorExpr(
            pgFieldColumn.getFieldName(), pgFieldColumn.getColumnName());
    return PostgresUtils.prepareCast(dataAccessor, type);
  }
}
