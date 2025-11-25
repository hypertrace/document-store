package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresFieldTypeDetector.FieldCategory;

class PostgresExistsRelationalFilterParser implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    // If true:
    // Regular fields -> IS NOT NULL
    // Arrays -> IS NOT NULL and cardinality(...) > 0,
    // JSONB arrays: Optimized GIN index query with containment check
    // If false:
    // Regular fields -> IS NULL
    // Arrays -> IS NULL OR cardinality(...) = 0,
    // JSONB arrays: COALESCE with array length check
    final boolean parsedRhs = !ConstantExpression.of(false).equals(expression.getRhs());

    FieldCategory category = expression.getLhs().accept(new PostgresFieldTypeDetector());

    switch (category) {
      case ARRAY:
        // First-class PostgreSQL array columns (text[], int[], etc.)
        return parsedRhs
            // We don't need to check that LHS is NOT NULL because WHERE cardinality(NULL) will not
            // be included in the result set
            ? String.format("(cardinality(%s) > 0)", parsedLhs)
            : String.format("COALESCE(cardinality(%s), 0) = 0", parsedLhs);

      case JSONB_ARRAY:
        {
          JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) expression.getLhs();
          String baseColumn = wrapWithDoubleQuotes(jsonExpr.getColumnName());
          String nestedPath = String.join(".", jsonExpr.getJsonPath());
          return parsedRhs
              // This is type-safe and will use the GIN index on parent JSONB col
              ? String.format(
                  "(%s @> '{\"" + nestedPath + "\": []}' AND jsonb_array_length(%s) > 0)",
                  baseColumn,
                  parsedLhs)
              // Return the number of elements in a JSONB array, default value of 0 if the array is
              // NULL
              : String.format("COALESCE(jsonb_array_length(%s), 0) = 0", parsedLhs);
        }

      case JSONB_SCALAR:
        {
          // JSONB scalar fields - use ? operator for GIN index optimization
          JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) expression.getLhs();
          String baseColumn = wrapWithDoubleQuotes(jsonExpr.getColumnName());
          String nestedPath = String.join(".", jsonExpr.getJsonPath());

          return parsedRhs
              ? String.format("%s ? '%s'", baseColumn, nestedPath)
              : String.format("NOT (%s ? '%s')", baseColumn, nestedPath);
        }

      case SCALAR:
      default:
        // Regular scalar fields - use standard NULL checks
        return parsedRhs
            ? String.format("%s IS NOT NULL", parsedLhs)
            : String.format("%s IS NULL", parsedLhs);
    }
  }

  private String wrapWithDoubleQuotes(String identifier) {
    return "\"" + identifier + "\"";
  }
}
