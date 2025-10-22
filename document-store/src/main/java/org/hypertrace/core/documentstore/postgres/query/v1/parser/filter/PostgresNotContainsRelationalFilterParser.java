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

    boolean useJsonParser = shouldUseJsonParser(expression, context);

    if (useJsonParser) {
      // Use the JSON logic for JSON document fields
      jsonContainsParser.parse(expression, context); // This adds the parameter.
      return String.format("%s IS NULL OR NOT %s @> ?::jsonb", parsedLhs, parsedLhs);
    } else {
      // Use the non-JSON logic for first-class fields
      String containsExpression = nonJsonContainsParser.parse(expression, context);
      return String.format("%s IS NULL OR NOT (%s)", parsedLhs, containsExpression);
    }
  }

  private boolean shouldUseJsonParser(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {

    boolean isJsonField = expression.getLhs() instanceof JsonIdentifierExpression;
    boolean isFlatCollection = context.getPgColTransformer().getDocumentType() == DocumentType.FLAT;
    boolean useJsonParser = !isFlatCollection || isJsonField;

    return useJsonParser;
  }
}
