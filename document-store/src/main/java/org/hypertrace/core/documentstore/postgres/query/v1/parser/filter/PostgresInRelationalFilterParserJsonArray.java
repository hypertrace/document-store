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
 *
 * <p>Special case: If the JSONB array field has been unnested, each row contains a scalar value
 * (not an array), so we use scalar IN syntax instead of the @> containment operator.
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

    // Check if this field has been unnested - if so, treat it as a scalar
    String fieldName = jsonExpr.getName();
    if (context.getPgColumnNames().containsKey(fieldName)) {
      // Field is unnested - each element is now a scalar, not an array
      // Use scalar IN operator instead of JSONB containment
      return prepareFilterStringForScalarInOperator(
          parsedLhs, parsedRhs, context.getParamsBuilder());
    }

    // Field is NOT unnested - use JSONB containment logic
    return prepareFilterStringForArrayInOperator(
        parsedLhs, parsedRhs, fieldType, context.getParamsBuilder());
  }

  /**
   * Generates SQL for scalar IN operator (used when JSONB array field has been unnested). Example:
   * "props_dot_source-loc" IN (?::jsonb, ?::jsonb)
   *
   * <p>Note: After unnesting with jsonb_array_elements(), each row contains a JSONB scalar value.
   * We cast the parameters to jsonb for direct JSONB-to-JSONB comparison, which works for all JSONB
   * types (strings, numbers, booleans, objects).
   */
  private String prepareFilterStringForScalarInOperator(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final Params.Builder paramsBuilder) {

    String placeholders =
        StreamSupport.stream(parsedRhs.spliterator(), false)
            .map(
                value -> {
                  // Add the value as a JSONB-formatted string
                  // For strings, this needs to be JSON-quoted (e.g., "warehouse-A" becomes
                  // "\"warehouse-A\"")
                  String jsonValue = convertToJsonString(value);
                  paramsBuilder.addObjectParam(jsonValue);
                  return "?::jsonb";
                })
            .collect(Collectors.joining(", "));

    // Direct JSONB comparison - no text conversion needed
    return String.format("%s IN (%s)", parsedLhs, placeholders);
  }

  /**
   * Converts a Java value to its JSON string representation for JSONB casting. Strings are quoted,
   * numbers/booleans are not.
   */
  private String convertToJsonString(Object value) {
    if (value == null) {
      return "null";
    } else if (value instanceof String) {
      // JSON strings must be quoted
      return "\"" + value.toString().replace("\"", "\\\"") + "\"";
    } else if (value instanceof Number || value instanceof Boolean) {
      // Numbers and booleans are not quoted in JSON
      return value.toString();
    } else {
      // For other types, assume they're already JSON-formatted or treat as string
      return "\"" + value.toString().replace("\"", "\\\"") + "\"";
    }
  }

  /**
   * Generates SQL for JSONB containment operator (used for non-unnested JSONB array fields).
   * Example: document->'tags' @> jsonb_build_array(?::text)
   */
  private String prepareFilterStringForArrayInOperator(
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
