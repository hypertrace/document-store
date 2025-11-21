package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserArrayField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserScalarField;

class PostgresInParserSelector implements SelectTypeExpressionVisitor {

  // Parsers for different expression types
  private static final PostgresInRelationalFilterParserInterface jsonFieldInFilterParser =
      new PostgresInRelationalFilterParser(); // Fallback for JSON without type info
  private static final PostgresInRelationalFilterParserInterface jsonPrimitiveInFilterParser =
      new PostgresInRelationalFilterParserJsonPrimitive(); // Optimized for JSON primitives
  private static final PostgresInRelationalFilterParserInterface jsonArrayInFilterParser =
      new PostgresInRelationalFilterParserJsonArray(); // Optimized for JSON arrays
  private static final PostgresInRelationalFilterParserInterface scalarFieldInFilterParser =
      new PostgresInRelationalFilterParserScalarField();
  private static final PostgresInRelationalFilterParserInterface arrayFieldInFilterParser =
      new PostgresInRelationalFilterParserArrayField();

  private final boolean isFlatCollection;

  PostgresInParserSelector(boolean isFlatCollection) {
    this.isFlatCollection = isFlatCollection;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(JsonIdentifierExpression expression) {
    // JsonFieldType is required for optimized SQL generation
    JsonFieldType fieldType = getFieldType(expression);

    switch (fieldType) {
      case STRING:
      case NUMBER:
      case BOOLEAN:
        // Primitives: use ->> (extract as text) with appropriate casting
        return jsonPrimitiveInFilterParser;
      case STRING_ARRAY:
      case NUMBER_ARRAY:
      case BOOLEAN_ARRAY:
      case OBJECT_ARRAY:
        // Typed arrays: use -> with @> and typed jsonb_build_array
        return jsonArrayInFilterParser;
      case OBJECT:
        // Objects: use -> with @> (future: needs separate parser)
        throw new UnsupportedOperationException(
            "IN operator on OBJECT type is not yet supported. Use primitive or array types.");
      default:
        throw new IllegalArgumentException("Unsupported JsonFieldType: " + fieldType);
    }
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(ArrayIdentifierExpression expression) {
    // Array fields with explicit ArrayIdentifierExpression use optimized array parser
    return isFlatCollection ? arrayFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(IdentifierExpression expression) {
    // IdentifierExpression: use scalar parser in flat, JSON parser in nested
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(AggregateExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(ConstantExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(DocumentConstantExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(FunctionExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(AliasedIdentifierExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  private static JsonFieldType getFieldType(JsonIdentifierExpression expression) {
    return expression
        .getFieldType()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "JsonFieldType must be specified for JsonIdentifierExpression in IN operations. "
                        + "Use JsonIdentifierExpression.of(column, JsonFieldType.*, path...)"));
  }
}
