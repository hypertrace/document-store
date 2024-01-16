package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareCast;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareFieldDataAccessorExpr;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

public class PostgresRelationalIdentifierAccessingExpressionVisitor
    extends PostgresSelectTypeExpressionVisitor {
  private final String baseField;
  private final SelectTypeExpression rhs;

  public PostgresRelationalIdentifierAccessingExpressionVisitor(
      final PostgresSelectTypeExpressionVisitor baseVisitor,
      final String baseField,
      final SelectTypeExpression rhs) {
    super(baseVisitor);
    this.baseField = baseField;
    this.rhs = rhs;
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return baseVisitor.getPostgresQueryParser();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    final String parsed = expression.accept(baseVisitor);
    final String dataAccessorExpr = prepareFieldDataAccessorExpr(parsed, baseField);
    final Object rhsValue = rhs.accept(new PostgresConstantExpressionVisitor());
    return prepareCast(dataAccessorExpr, rhsValue);
  }
}
