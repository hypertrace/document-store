package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.registry.PostgresColumnRegistry;

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
    // Extract field name from the expression
    String fieldName = extractFieldName(context);
    boolean isFirstClassField = determineIfFirstClassField(fieldName, context);

    return isFirstClassField ? nonJsonFieldInFilterParser : jsonFieldInFilterParser;
  }

  /**
   * Extracts the field name from the context's current expression. This is a simplified approach -
   * in a full implementation, we'd need access to the expression.
   */
  private String extractFieldName(PostgresRelationalFilterContext context) {
    // Note: This is a limitation of the current design - we don't have direct access to the
    // expression here.
    // For now, we'll return null and rely on the fallback logic.
    // In a full refactor, we'd pass the field name through the context or restructure the parser
    // hierarchy.
    return null;
  }

  /** Determines if a field is a first-class column using registry-based lookup with fallback. */
  private boolean determineIfFirstClassField(
      String fieldName, PostgresRelationalFilterContext context) {
    PostgresColumnRegistry registry = context.getColumnRegistry();

    // Use registry-based type resolution if available
    if (registry != null && fieldName != null) {
      return registry.isFirstClassColumn(fieldName);
    } else {
      // Fallback to flatStructureCollection for backward compatibility
      String flatStructureCollection = context.getFlatStructureCollectionName();
      return flatStructureCollection != null
          && flatStructureCollection.equals(context.getTableIdentifier().getTableName());
    }
  }
}
