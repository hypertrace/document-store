package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresNotInRelationalFilterParser implements PostgresRelationalFilterParser {
  private static final PostgresInRelationalFilterParser inRelationalFilterParser =
      new PostgresInRelationalFilterParser();

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final String parsedInExpression = inRelationalFilterParser.parse(expression, context);
    return String.format("%s IS NULL OR NOT (%s)", parsedLhs, parsedInExpression);
  }
}
