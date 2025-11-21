package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;

/**
 * Optimized parser for IN operations on JSON array fields with type-specific casting.
 *
 * <p>Uses JSONB containment operator (@>) with typed jsonb_build_array for "contains any"
 * semantics:
 *
 * <ul>
 *   <li><b>STRING_ARRAY:</b> {@code "document" -> 'tags' @> jsonb_build_array(?::text)}
 *   <li><b>NUMBER_ARRAY:</b> {@code "document" -> 'scores' @> jsonb_build_array(?::numeric)}
 *   <li><b>BOOLEAN_ARRAY:</b> {@code "document" -> 'flags' @> jsonb_build_array(?::boolean)}
 *   <li><b>OBJECT_ARRAY:</b> {@code "document" -> 'items' @> jsonb_build_array(?::jsonb)}
 * </ul>
 *
 * <p>This checks if the JSON array contains ANY of the provided values, using efficient JSONB
 * containment instead of defensive type checking.
 */
public class PostgresInRelationalFilterParserJsonArray
    implements PostgresInRelationalFilterParserInterface {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Iterable<Object> parsedRhs = expression.getRhs().accept(context.rhsParser());

    // Extract field type for typed array handling (guaranteed to be present by selector)
    JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) expression.getLhs();
    JsonFieldType fieldType =
        jsonExpr
            .getFieldType()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "JsonFieldType must be present - this should have been caught by the selector"));

    return prepareFilterStringForInOperator(
        parsedLhs, parsedRhs, fieldType, context.getParamsBuilder());
  }

  private String prepareFilterStringForInOperator(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final JsonFieldType fieldType,
      final Params.Builder paramsBuilder) {

    // Determine the appropriate type cast for jsonb_build_array elements
    String typeCast = getTypeCastForArray(fieldType);

    // For JSON arrays, we use the @> containment operator
    // Check if ANY of the RHS values is contained in the LHS array
    String orConditions =
        StreamSupport.stream(parsedRhs.spliterator(), false)
            .map(
                value -> {
                  paramsBuilder.addObjectParam(value);
                  return String.format("%s @> jsonb_build_array(?%s)", parsedLhs, typeCast);
                })
            .collect(Collectors.joining(" OR "));

    // Wrap in parentheses if multiple conditions
    return StreamSupport.stream(parsedRhs.spliterator(), false).count() > 1
        ? String.format("(%s)", orConditions)
        : orConditions;
  }

  /**
   * Returns the PostgreSQL type cast string for jsonb_build_array elements based on array type.
   *
   * @param fieldType The JSON field type (must not be null)
   * @return Type cast string (e.g., "::text", "::numeric")
   */
  private String getTypeCastForArray(JsonFieldType fieldType) {
    switch (fieldType) {
      case STRING_ARRAY:
        return "::text";
      case NUMBER_ARRAY:
        return "::numeric";
      case BOOLEAN_ARRAY:
        return "::boolean";
      case OBJECT_ARRAY:
        return "::jsonb";
      default:
        throw new IllegalArgumentException(
            "Unsupported array type: " + fieldType + ". Expected *_ARRAY types.");
    }
  }
}
