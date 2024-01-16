package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresNotExistsRelationalFilterParser implements PostgresRelationalFilterParser {
  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final boolean parsedRhs = ConstantExpression.of(false).equals(expression.getRhs());
    return parsedRhs
        ? String.format("%s IS NOT NULL", parsedLhs)
        : String.format("%s IS NULL", parsedLhs);
  }
}
