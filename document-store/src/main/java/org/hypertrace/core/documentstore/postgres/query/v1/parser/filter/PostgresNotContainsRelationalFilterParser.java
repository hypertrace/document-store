package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresContainsRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.registry.PostgresColumnRegistry;

class PostgresNotContainsRelationalFilterParser implements PostgresRelationalFilterParser {
  private static final PostgresContainsRelationalFilterParser jsonContainsParser =
      new PostgresContainsRelationalFilterParser();
  private static final PostgresContainsRelationalFilterParserNonJsonField nonJsonContainsParser =
      new PostgresContainsRelationalFilterParserNonJsonField();

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());

    // Extract field name and determine if it's a first-class field
    String fieldName = extractFieldName(expression);
    boolean isFirstClassField = determineIfFirstClassField(fieldName, context);

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

  /** Extracts the field name from the left-hand side of a RelationalExpression. */
  private String extractFieldName(RelationalExpression expression) {
    if (expression.getLhs() instanceof IdentifierExpression) {
      return ((IdentifierExpression) expression.getLhs()).getName();
    }
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
