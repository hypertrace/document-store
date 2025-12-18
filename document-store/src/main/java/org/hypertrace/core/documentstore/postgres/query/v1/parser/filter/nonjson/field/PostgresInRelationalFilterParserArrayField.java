package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresInRelationalFilterParserInterface;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of PostgresInRelationalFilterParserInterface for handling IN operations on array
 * fields (non-JSON array columns), using the PostgreSQL array overlap operator (&&).
 *
 * <p>For array fields like "tags", the IN operator semantics are: "does the array contain ANY of
 * the provided values?" This is implemented using the PostgreSQL array overlap operator (&&).
 *
 * <p>Example: tags IN ('hygiene', 'premium') translates to: tags && ARRAY['hygiene',
 * 'premium']::text[]
 *
 * <p>Special case: If the array field has been unnested, each row contains a scalar value (not an
 * array), so we use scalar IN syntax instead of the array overlap operator.
 */
public class PostgresInRelationalFilterParserArrayField
    implements PostgresInRelationalFilterParserInterface {

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Iterable<Object> parsedRhs = expression.getRhs().accept(context.rhsParser());

    // Extract element type from expression metadata for type-safe query generation
    String sqlType = expression.getLhs().accept(PostgresTypeExtractor.scalarType());

    // Check if this field has been unnested - if so, treat it as a scalar
    ArrayIdentifierExpression arrayExpr = (ArrayIdentifierExpression) expression.getLhs();
    String fieldName = arrayExpr.getName();
    if (context.getPgColumnNames().containsKey(fieldName)) {
      // Field is unnested - each element is now a scalar, not an array
      // Use scalar IN operator instead of array overlap
      return prepareFilterStringForScalarInOperator(
          parsedLhs, parsedRhs, sqlType, context.getParamsBuilder());
    }

    // Field is NOT unnested - use array overlap logic
    return prepareFilterStringForArrayInOperator(
        parsedLhs, parsedRhs, sqlType, context.getParamsBuilder());
  }

  /**
   * Generates SQL for scalar IN operator (used when array field has been unnested). Example:
   * "tags_unnested" = ANY(?)
   */
  private String prepareFilterStringForScalarInOperator(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final String sqlType,
      final Params.Builder paramsBuilder) {
    // If type is specified, use optimized ANY(ARRAY[]) syntax
    // Otherwise, fall back to traditional IN (?, ?, ?) for backward compatibility
    if (sqlType != null) {
      Object[] values = StreamSupport.stream(parsedRhs.spliterator(), false).toArray();

      if (values.length == 0) {
        return "1 = 0";
      }

      paramsBuilder.addArrayParam(values, sqlType);
      return String.format("%s = ANY(?)", parsedLhs);
    } else {
      return prepareFilterStringFallback(parsedLhs, parsedRhs, paramsBuilder, "%s IN (%s)");
    }
  }

  /**
   * Generates SQL for array overlap operator (used for non-unnested array fields). Example: "tags"
   * && ?
   *
   * <p>Uses a single array parameter.
   */
  private String prepareFilterStringForArrayInOperator(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final String sqlType,
      final Params.Builder paramsBuilder) {
    // If type is specified, use optimized array overlap with typed array
    // Otherwise, fall back to jsonb-based approach for backward compatibility
    if (sqlType != null) {
      Object[] values = StreamSupport.stream(parsedRhs.spliterator(), false).toArray();

      if (values.length == 0) {
        return "1 = 0";
      }

      paramsBuilder.addArrayParam(values, sqlType);
      return String.format("%s && ?", parsedLhs);
    } else {
      // Fallback: cast both sides to text[] for backward compatibility with any array type
      return prepareFilterStringFallback(
          parsedLhs, parsedRhs, paramsBuilder, "%s::text[] && ARRAY[%s]::text[]");
    }
  }

  /**
   * Fallback method using traditional (?, ?, ?) syntax for backward compatibility when type
   * information is not available.
   */
  private String prepareFilterStringFallback(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final Params.Builder paramsBuilder,
      final String formatPattern) {

    String collect =
        StreamSupport.stream(parsedRhs.spliterator(), false)
            .map(
                val -> {
                  paramsBuilder.addObjectParam(val);
                  return "?";
                })
            .collect(Collectors.joining(", "));

    if (collect.isEmpty()) {
      return "1 = 0";
    }

    return String.format(formatPattern, parsedLhs, collect);
  }
}
