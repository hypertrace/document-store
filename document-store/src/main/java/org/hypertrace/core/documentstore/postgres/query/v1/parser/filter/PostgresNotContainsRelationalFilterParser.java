package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresContainsRelationalFilterParser.prepareJsonValueForContainsOp;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresNotContainsRelationalFilterParser implements PostgresRelationalFilterParser {
  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    final Object convertedRhs = prepareJsonValueForContainsOp(parsedRhs);
    context.getParamsBuilder().addObjectParam(convertedRhs);

    return String.format("%s IS NULL OR NOT %s @> ?::jsonb", parsedLhs, parsedLhs);
  }
}
