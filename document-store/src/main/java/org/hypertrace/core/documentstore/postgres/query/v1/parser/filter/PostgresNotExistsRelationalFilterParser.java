package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresFieldTypeDetector.FieldCategory;

class PostgresNotExistsRelationalFilterParser implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    // If true (RHS = false):
    // Regular fields -> IS NOT NULL
    // Arrays -> IS NOT NULL AND cardinality(...) > 0
    // JSONB arrays: Optimized GIN index query with containment check
    // If false (RHS = true or other):
    // Regular fields -> IS NULL
    // Arrays -> IS NULL OR cardinality(...) = 0
    // JSONB arrays: COALESCE with array length check
    final boolean parsedRhs = ConstantExpression.of(false).equals(expression.getRhs());

    FieldCategory category = expression.getLhs().accept(new PostgresFieldTypeDetector());

    switch (category) {
      case ARRAY:
        {
          // For first-class array fields, only return those arrays that are not null and have
          // at-least 1 element in it (so exclude NULL or empty arrays). This is to match Mongo's
          // behavior
          // Check if this field has been unnested - if so, treat it as a scalar
          IdentifierExpression arrayExpr = (IdentifierExpression) expression.getLhs();
          String arrayFieldName = arrayExpr.getName();
          if (context.getPgColumnNames().containsKey(arrayFieldName)) {
            // Field is unnested - each element is now a scalar, not an array
            // Use simple NULL checks instead of cardinality
            return getScalarExpr(parsedRhs, parsedLhs);
          }

          // Field is NOT unnested - apply cardinality logic
          return parsedRhs
              ? String.format("(cardinality(%s) > 0)", parsedLhs)
              // More efficient than: %s IS NULL OR cardinality(%s) = 0)? as we can create
              // an index on the COALESCE function itself which will return in a single
              // index seek rather than two index seeks in the OR query
              : String.format("COALESCE(cardinality(%s), 0) = 0", parsedLhs);
        }

      case JSONB_ARRAY:
        {
          // Arrays inside JSONB columns - use optimized GIN index queries
          JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) expression.getLhs();
          // Check if this field has been unnested - if so, treat it as a scalar
          String fieldName = jsonExpr.getName();
          if (context.getPgColumnNames().containsKey(fieldName)) {
            // Field is unnested - each element is now a scalar, not an array
            // Use simple NULL checks instead of array length
            return getScalarExpr(parsedRhs, parsedLhs);
          }

          // Field is NOT unnested - apply array length logic
          String baseColumn = wrapWithDoubleQuotes(jsonExpr.getColumnName());
          String nestedPath = String.join(".", jsonExpr.getJsonPath());

          return parsedRhs
              ? String.format(
                  "(%s @> '{\"" + nestedPath + "\": []}' AND jsonb_array_length(%s) > 0)",
                  baseColumn,
                  parsedLhs)
              : String.format("COALESCE(jsonb_array_length(%s), 0) = 0", parsedLhs);
        }

      case JSONB_SCALAR:
      case SCALAR:
      default:
        return getScalarExpr(parsedRhs, parsedLhs);
    }
  }

  private static String getScalarExpr(boolean parsedRhs, String parsedLhs) {
    return parsedRhs
        ? String.format("%s IS NOT NULL", parsedLhs)
        : String.format("%s IS NULL", parsedLhs);
  }

  private String wrapWithDoubleQuotes(String identifier) {
    return "\"" + identifier + "\"";
  }
}
