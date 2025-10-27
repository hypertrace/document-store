package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserNonJsonField;

class PostgresNotInRelationalFilterParser implements PostgresRelationalFilterParser {

  private static final PostgresInRelationalFilterParserInterface jsonFieldInFilterParser =
      new PostgresInRelationalFilterParser();
  private static final PostgresInRelationalFilterParserInterface nonJsonFieldInFilterParser =
      new PostgresInRelationalFilterParserNonJsonField();

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());

    PostgresInRelationalFilterParserInterface inFilterParser =
        getInFilterParser(expression, context);

    final String parsedInExpression = inFilterParser.parse(expression, context);
    return String.format("%s IS NULL OR NOT (%s)", parsedLhs, parsedInExpression);
  }

  private PostgresInRelationalFilterParserInterface getInFilterParser(
      final RelationalExpression expression, PostgresRelationalFilterContext context) {
    // Check if LHS is a JSON field (JSONB column access)
    boolean isJsonField = expression.getLhs() instanceof JsonIdentifierExpression;

    // Check if the collection type is flat or nested
    boolean isFlatCollection = context.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    // Use JSON parser for:
    // 1. Nested collections - !isFlatCollection
    // 2. JSON fields within flat collections - isJsonField
    boolean useJsonParser = !isFlatCollection || isJsonField;

    return useJsonParser ? jsonFieldInFilterParser : nonJsonFieldInFilterParser;
  }
}
