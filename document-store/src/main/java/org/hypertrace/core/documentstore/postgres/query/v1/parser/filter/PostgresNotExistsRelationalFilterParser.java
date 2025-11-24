package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
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
    // JSONB arrays: IS NOT NULL AND jsonb_typeof(%s) = 'array' AND jsonb_array_length(...) > 0
    // If false (RHS = true or other):
    // Regular fields -> IS NULL
    // Arrays -> IS NULL OR cardinality(...) = 0
    // JSONB arrays: IS NULL OR (jsonb_typeof(%s) = 'array' AND jsonb_array_length(...) = 0)
    final boolean parsedRhs = ConstantExpression.of(false).equals(expression.getRhs());

    FieldCategory category = expression.getLhs().accept(new PostgresFieldTypeDetector());

    switch (category) {
      case ARRAY:
        // For first-class array fields, only return those arrays that are not null and have
        // at-least 1 element in it (so exclude NULL or empty arrays). This is to match Mongo's
        // behavior
        return parsedRhs
            ? String.format("(%s IS NOT NULL AND cardinality(%s) > 0)", parsedLhs, parsedLhs)
            : String.format("(%s IS NULL OR cardinality(%s) = 0)", parsedLhs, parsedLhs);

      case JSONB_ARRAY:
        return parsedRhs
            ? String.format(
                "(%s IS NOT NULL AND jsonb_typeof(%s) = 'array' AND jsonb_array_length(%s) > 0)",
                parsedLhs, parsedLhs, parsedLhs)
            : String.format(
                "(%s IS NULL OR (jsonb_typeof(%s) = 'array' AND jsonb_array_length(%s) = 0))",
                parsedLhs, parsedLhs, parsedLhs);

      case SCALAR:
      default:
        // Regular scalar fields
        return parsedRhs
            ? String.format("%s IS NOT NULL", parsedLhs)
            : String.format("%s IS NULL", parsedLhs);
    }
  }
}
