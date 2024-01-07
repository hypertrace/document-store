package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@AllArgsConstructor
public class PostgresDocumentArrayWrappingFilterVisitorProvider
    implements PostgresWrappingFilterVisitorProvider {

  private PostgresQueryParser postgresQueryParser;
  private String alias;

  @Override
  public PostgresSelectTypeExpressionVisitor getForRelational(
      final PostgresSelectTypeExpressionVisitor baseVisitor, final SelectTypeExpression rhs) {
    // Override the existing parser,
    // parse the field name (meta.num_moons),
    // get data accessor expression prefixed with alias (planets->'meta'->>'num_moons'), and
    // cast according to the value type (CAST (planets->'meta'->>'num_moons' AS NUMERIC) > ?)
    return new PostgresRelationalIdentifierAccessingExpressionVisitor(
        new PostgresIdentifierExpressionVisitor(postgresQueryParser), alias, rhs);
  }

  @Override
  public PostgresSelectTypeExpressionVisitor getForNonRelational(
      final PostgresSelectTypeExpressionVisitor baseVisitor) {
    // Any LHS field name (elements) is to be prefixed with current alias (inner)
    return new PostgresIdentifierAccessingExpressionVisitor(baseVisitor, alias);
  }
}
