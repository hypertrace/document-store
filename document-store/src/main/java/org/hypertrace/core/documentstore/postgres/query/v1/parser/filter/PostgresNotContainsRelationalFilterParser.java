package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

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

    String flatStructureCollection = context.getFlatStructureCollectionName();
    boolean isFirstClassField =
        flatStructureCollection != null
            && flatStructureCollection.equals(context.getTableIdentifier().getTableName());

    if (isFirstClassField) {
      // Use the non-JSON logic for first-class fields
      String containsExpression = nonJsonContainsParser.parse(expression, context);
      return String.format("%s IS NULL OR NOT (%s)", parsedLhs, containsExpression);
    } else {
      // Use the JSON logic for document fields.
      jsonContainsParser.parse(expression, context); // This adds the parameter.
      return String.format("%s IS NULL OR NOT %s @> ?::jsonb", parsedLhs, parsedLhs);
    }
  }
}
