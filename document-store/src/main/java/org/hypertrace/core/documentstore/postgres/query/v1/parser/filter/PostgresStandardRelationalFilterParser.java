package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresStandardRelationalFilterParser implements PostgresRelationalFilterParser {
  private static final PostgresStandardRelationalOperatorMapper mapper =
      new PostgresStandardRelationalOperatorMapper();

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final Object parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    final String operator = mapper.getMapping(expression.getOperator(), parsedRhs);

    if (parsedRhs != null) {
      context.getParamsBuilder().addObjectParam(parsedRhs);
      return String.format("%s %s ?", parsedLhs, operator);
    } else {
      return String.format("%s %s NULL", parsedLhs, operator);
    }
  }
}
