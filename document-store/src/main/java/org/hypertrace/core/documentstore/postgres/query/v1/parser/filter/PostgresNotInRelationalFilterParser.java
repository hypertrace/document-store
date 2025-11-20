package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresNotInRelationalFilterParser implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());

    boolean isFlatCollection = context.getPgColTransformer().getDocumentType() == DocumentType.FLAT;
    PostgresInRelationalFilterParserInterface inFilterParser =
        expression.getLhs().accept(new PostgresNotInParserSelector(isFlatCollection));

    final String parsedInExpression = inFilterParser.parse(expression, context);
    return String.format("%s IS NULL OR NOT (%s)", parsedLhs, parsedInExpression);
  }
}
