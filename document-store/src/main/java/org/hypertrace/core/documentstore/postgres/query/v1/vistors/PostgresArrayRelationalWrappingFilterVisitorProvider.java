package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@AllArgsConstructor
public class PostgresArrayRelationalWrappingFilterVisitorProvider
    implements PostgresWrappingFilterVisitorProvider {

  private PostgresQueryParser postgresQueryParser;
  private String arraySource;
  private String alias;

  @Override
  public PostgresSelectTypeExpressionVisitor getForRelational(
      final PostgresSelectTypeExpressionVisitor baseVisitor, final SelectTypeExpression rhs) {
    // Override the base visitor,
    // pick the LHS field name (elements.inner),
    // replace it with alias (inner), and
    // optionally trim double quotes (TRIM('"' FROM inner::text))
    return new PostgresIdentifierTrimmingExpressionVisitor(
        new PostgresIdentifierReplacingExpressionVisitor(
            new PostgresIdentifierExpressionVisitor(postgresQueryParser), arraySource, alias),
        rhs);
  }

  @Override
  public PostgresSelectTypeExpressionVisitor getForNonRelational(
      PostgresSelectTypeExpressionVisitor baseVisitor) {
    return getForRelational(baseVisitor, null);
  }
}
