package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresInRelationalFilterParserInterface;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

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

    // Check if this field has been unnested - if so, treat it as a scalar
    ArrayIdentifierExpression arrayExpr = (ArrayIdentifierExpression) expression.getLhs();
    String fieldName = arrayExpr.getName();
    if (context.getPgColumnNames().containsKey(fieldName)) {
      // Field is unnested - each element is now a scalar, not an array
      // Use scalar IN operator instead of array overlap
      return prepareFilterStringForScalarInOperator(
          parsedLhs, parsedRhs, context.getParamsBuilder());
    }

    // Field is NOT unnested - use array overlap logic
    String arrayTypeCast = expression.getLhs().accept(new PostgresArrayTypeExtractor());
    return prepareFilterStringForArrayInOperator(
        parsedLhs, parsedRhs, arrayTypeCast, context.getParamsBuilder());
  }

  /**
   * Generates SQL for scalar IN operator (used when array field has been unnested). Example:
   * "tags_unnested" = ANY(?)
   */
  private String prepareFilterStringForScalarInOperator(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final Params.Builder paramsBuilder) {

    Object[] values = StreamSupport.stream(parsedRhs.spliterator(), false).toArray();

    if (values.length == 0) {
      // return FALSE
      return "1 = 0";
    }

    // SQL type is needed during parameter binding
    paramsBuilder.addArrayParam(values, PostgresUtils.inferSqlTypeFromValue(values));

    return String.format("%s = ANY(?)", parsedLhs);
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
      final String arrayType,
      final Params.Builder paramsBuilder) {

    // Collect all values into an array
    Object[] values = StreamSupport.stream(parsedRhs.spliterator(), false).toArray();

    if (values.length == 0) {
      return "1 = 0";
    }

    // Infer SQL type from first value or array type hint
    String sqlType =
        arrayType != null
            ? mapArrayTypeToSqlType(arrayType)
            : PostgresUtils.inferSqlTypeFromValue(values);

    // Add as single array parameter
    paramsBuilder.addArrayParam(values, sqlType);

    // Use array overlap operator with single parameter
    return String.format("%s && ?", parsedLhs);
  }

  private String mapArrayTypeToSqlType(String arrayType) {
    // Remove [] suffix
    String baseType = arrayType.replace("[]", "");

    // Map to internal type names for createArrayOf()
    switch (baseType) {
      case "double precision":
        return "float8";
      case "integer":
        return "int4";
      case "boolean":
        return "bool";
      case "text":
        return "text";
      default:
        return baseType;
    }
  }
}
