package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresContainsRelationalFilterParserNonJsonField;

class PostgresNotContainsRelationalFilterParser implements PostgresRelationalFilterParser {

  private static final PostgresContainsRelationalFilterParser jsonContainsParser =
      new PostgresContainsRelationalFilterParser();
  private static final PostgresContainsRelationalFilterParserNonJsonField nonJsonContainsParser =
      new PostgresContainsRelationalFilterParserNonJsonField();

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());

    // Check if LHS is a JSON field (JSONB column access)
    boolean isJsonField = expression.getLhs() instanceof JsonIdentifierExpression;

    // Check if the collection type is flat or nested
    boolean isFlatCollection = context.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    // Use JSON parser for:
    // 1. Nested collections - !isFlatCollection
    // 2. JSON fields within flat collections - isJsonField
    boolean useJsonParser = !isFlatCollection || isJsonField;

    if (useJsonParser) {
      // Use the JSON logic for JSON document fields
      jsonContainsParser.parse(expression, context); // This adds the parameter.
      return String.format("%s IS NULL OR NOT %s @> ?::jsonb", parsedLhs, parsedLhs);
    } else {
      // Use the non-JSON logic for first-class array fields
      String containsExpression = nonJsonContainsParser.parse(expression, context);
      return String.format("%s IS NULL OR NOT (%s)", parsedLhs, containsExpression);
    }
  }
}
