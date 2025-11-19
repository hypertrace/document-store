package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresNotExistsRelationalFilterParser implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    // If true (RHS = false):
    // Regular fields -> IS NOT NULL
    // Arrays -> IS NOT NULL AND cardinality(...) > 0
    // JSONB arrays: IS NOT NULL AND jsonb_typeof(%s) = 'array' AND jsonb_array_length(...) > 0
    // If false (RHS = true or other):
    // Regular fields -> IS NULL
    // Arrays -> IS NULL OR cardinality(...) = 0
    // JSONB arrays: IS NULL OR jsonb_typeof(%s) != 'array' OR jsonb_array_length(...) = 0
    final boolean parsedRhs = ConstantExpression.of(false).equals(expression.getRhs());

    // For array fields, NOT_EXISTS should check NULL or empty
    boolean isArrayField = expression.getLhs() instanceof ArrayIdentifierExpression;
    boolean isJsonArrayField = expression.getLhs() instanceof JsonArrayIdentifierExpression;

    if (isArrayField) {
      // First-class PostgreSQL array columns (text[], int[], etc.)
      return parsedRhs
          ? String.format("(%s IS NOT NULL AND cardinality(%s) > 0)", parsedLhs, parsedLhs)
          : String.format("(%s IS NULL OR cardinality(%s) = 0)", parsedLhs, parsedLhs);
    } else if (isJsonArrayField) {
      // Arrays inside JSONB columns
      return parsedRhs
          ? String.format(
              "(%s IS NOT NULL AND jsonb_typeof(%s) = 'array' AND jsonb_array_length(%s) > 0)",
              parsedLhs, parsedLhs, parsedLhs)
          : String.format(
              "(%s IS NULL OR jsonb_typeof(%s) != 'array' OR jsonb_array_length(%s) = 0)",
              parsedLhs, parsedLhs, parsedLhs);
    } else {
      // Regular scalar fields
      return parsedRhs
          ? String.format("%s IS NOT NULL", parsedLhs)
          : String.format("%s IS NULL", parsedLhs);
    }
  }
}
