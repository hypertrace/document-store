package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresStartsWithRelationalFilterParser implements PostgresRelationalFilterParser {
  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    context.getParamsBuilder().addObjectParam(parsedRhs);
    return String.format("%s::text ^@ ?", parsedLhs);
  }
}
