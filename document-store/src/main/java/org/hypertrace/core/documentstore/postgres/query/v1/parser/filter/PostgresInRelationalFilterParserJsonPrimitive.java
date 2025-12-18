package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;

/**
 * Optimized parser for IN operations on JSON primitive fields (string, number, boolean) with proper
 * type casting.
 *
 * <p>Generates efficient SQL using {@code ->>} operator with {@code = ANY(ARRAY[])} and appropriate
 * PostgreSQL casting:
 *
 * <ul>
 *   <li><b>STRING:</b> {@code "document" ->> 'item' = ANY(ARRAY['Soap', 'Shampoo'])}
 *   <li><b>NUMBER:</b> {@code CAST("document" ->> 'price' AS NUMERIC) = ANY(ARRAY[10, 20])}
 *   <li><b>BOOLEAN:</b> {@code CAST("document" ->> 'active' AS BOOLEAN) = ANY(ARRAY[true, false])}
 * </ul>
 *
 * <p>This is much more efficient than the defensive approach that checks both array and scalar
 * types, and ensures correct type comparisons.
 */
public class PostgresInRelationalFilterParserJsonPrimitive
    implements PostgresInRelationalFilterParserInterface {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Iterable<Object> parsedRhs = expression.getRhs().accept(context.rhsParser());

    // Extract field type for proper casting (guaranteed to be present by selector)
    JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) expression.getLhs();
    JsonFieldType fieldType =
        jsonExpr
            .getFieldType()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "JsonFieldType must be present - this should have been caught by the selector"));

    // For JSON primitives, we need ->> (text extraction) instead of -> (jsonb extraction)
    // The LHS parser generates: "props"->'brand' (returns JSONB)
    // We need: "props"->>'brand' (returns TEXT)
    // Replace the last -> with ->> for primitive type extraction
    int lastArrowIndex = parsedLhs.lastIndexOf("->");
    if (lastArrowIndex != -1) {
      parsedLhs =
          parsedLhs.substring(0, lastArrowIndex) + "->>" + parsedLhs.substring(lastArrowIndex + 2);
    }

    return prepareFilterStringForInOperator(
        parsedLhs, parsedRhs, fieldType, context.getParamsBuilder());
  }

  private String prepareFilterStringForInOperator(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final JsonFieldType fieldType,
      final Params.Builder paramsBuilder) {

    Object[] values = StreamSupport.stream(parsedRhs.spliterator(), false).toArray();

    if (values.length == 0) {
      // return FALSE
      return "1 = 0";
    }

    String sqlType = mapJsonFieldTypeToSqlType(fieldType);
    paramsBuilder.addArrayParam(values, sqlType);

    // Apply appropriate casting based on field type
    String lhsWithCast = parsedLhs;
    if (fieldType == JsonFieldType.NUMBER) {
      lhsWithCast = String.format("CAST(%s AS NUMERIC)", parsedLhs);
    } else if (fieldType == JsonFieldType.BOOLEAN) {
      lhsWithCast = String.format("CAST(%s AS BOOLEAN)", parsedLhs);
    }
    return String.format("%s = ANY(?)", lhsWithCast);
  }

  private String mapJsonFieldTypeToSqlType(JsonFieldType fieldType) {
    switch (fieldType) {
      case NUMBER:
        return "float8";
      case BOOLEAN:
        return "bool";
      case STRING:
      default:
        return "text";
    }
  }
}
