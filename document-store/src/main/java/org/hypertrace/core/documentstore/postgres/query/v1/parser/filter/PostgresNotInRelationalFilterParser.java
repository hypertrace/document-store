package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

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

    PostgresInRelationalFilterParserInterface inFilterParser = getInFilterParser(context);

    final String parsedInExpression = inFilterParser.parse(expression, context);
    return String.format("%s IS NULL OR NOT (%s)", parsedLhs, parsedInExpression);
  }

  private PostgresInRelationalFilterParserInterface getInFilterParser(
      PostgresRelationalFilterContext context) {
    String flatStructureCollection = context.getFlatStructureCollectionName();
    boolean isFirstClassField =
        flatStructureCollection != null
            && flatStructureCollection.equals(context.getTableIdentifier().getTableName());

    return isFirstClassField ? nonJsonFieldInFilterParser : jsonFieldInFilterParser;
  }
}
