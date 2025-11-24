package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_EXISTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayType;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser.PostgresRelationalFilterContext;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresNotExistsRelationalFilterParserTest {

  private PostgresNotExistsRelationalFilterParser parser;
  private PostgresRelationalFilterContext context;
  private PostgresSelectTypeExpressionVisitor lhsParser;

  @BeforeEach
  void setUp() {
    parser = new PostgresNotExistsRelationalFilterParser();
    context = mock(PostgresRelationalFilterContext.class);
    lhsParser = mock(PostgresSelectTypeExpressionVisitor.class);
    when(context.lhsParser()).thenReturn(lhsParser);
  }

  @Test
  void testParse_arrayField_rhsFalse() {
    // Test NOT_EXISTS on array with RHS = false (means NOT_EXISTS should be true)
    ArrayIdentifierExpression lhs = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);
    ConstantExpression rhs = ConstantExpression.of(false);
    RelationalExpression expression = RelationalExpression.of(lhs, NOT_EXISTS, rhs);

    when(lhsParser.visit(any(ArrayIdentifierExpression.class))).thenReturn("\"tags\"");

    String result = parser.parse(expression, context);

    assertEquals(
        "(\"tags\" IS NOT NULL AND cardinality(\"tags\") > 0)",
        result,
        "NOT_EXISTS with RHS=false on ARRAY should check IS NOT NULL AND cardinality > 0");
  }

  @Test
  void testParse_arrayField_rhsTrue() {
    // Test NOT_EXISTS on array with RHS = true (means NOT_EXISTS should be false)
    ArrayIdentifierExpression lhs = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);
    ConstantExpression rhs = ConstantExpression.of("null"); // Any non-false value
    RelationalExpression expression = RelationalExpression.of(lhs, NOT_EXISTS, rhs);

    when(lhsParser.visit(any(ArrayIdentifierExpression.class))).thenReturn("\"tags\"");

    String result = parser.parse(expression, context);

    assertEquals(
        "(\"tags\" IS NULL OR cardinality(\"tags\") = 0)",
        result,
        "NOT_EXISTS with RHS=true on ARRAY should check IS NULL OR cardinality = 0");
  }

  @Test
  void testParse_jsonbArrayField_rhsFalse() {
    // Test NOT_EXISTS on JSONB array with RHS = false
    JsonIdentifierExpression lhs =
        JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors");
    ConstantExpression rhs = ConstantExpression.of(false);
    RelationalExpression expression = RelationalExpression.of(lhs, NOT_EXISTS, rhs);

    when(lhsParser.visit(any(JsonIdentifierExpression.class)))
        .thenReturn("document->'props'->'colors'");

    String result = parser.parse(expression, context);

    assertEquals(
        "(document->'props'->'colors' IS NOT NULL AND jsonb_typeof(document->'props'->'colors') = 'array' AND jsonb_array_length(document->'props'->'colors') > 0)",
        result,
        "NOT_EXISTS with RHS=false on JSONB_ARRAY should check IS NOT NULL AND typeof = 'array' AND length > 0");
  }

  @Test
  void testParse_jsonbArrayField_rhsTrue() {
    // Test NOT_EXISTS on JSONB array with RHS = true
    JsonIdentifierExpression lhs =
        JsonIdentifierExpression.of("props", JsonFieldType.BOOLEAN_ARRAY, "flags");
    ConstantExpression rhs = ConstantExpression.of("null");
    RelationalExpression expression = RelationalExpression.of(lhs, NOT_EXISTS, rhs);

    when(lhsParser.visit(any(JsonIdentifierExpression.class)))
        .thenReturn("document->'props'->'flags'");

    String result = parser.parse(expression, context);

    assertEquals(
        "(document->'props'->'flags' IS NULL OR (jsonb_typeof(document->'props'->'flags') = 'array' AND jsonb_array_length(document->'props'->'flags') = 0))",
        result,
        "NOT_EXISTS with RHS=true on JSONB_ARRAY should check IS NULL OR (typeof = 'array' AND length = 0)");
  }

  @Test
  void testParse_scalarField_rhsFalse() {
    // Test NOT_EXISTS on scalar field with RHS = false
    IdentifierExpression lhs = IdentifierExpression.of("item");
    ConstantExpression rhs = ConstantExpression.of(false);
    RelationalExpression expression = RelationalExpression.of(lhs, NOT_EXISTS, rhs);

    when(lhsParser.visit(any(IdentifierExpression.class))).thenReturn("document->>'item'");

    String result = parser.parse(expression, context);

    assertEquals(
        "document->>'item' IS NOT NULL",
        result,
        "NOT_EXISTS with RHS=false on SCALAR should check IS NOT NULL");
  }

  @Test
  void testParse_scalarField_rhsTrue() {
    // Test NOT_EXISTS on scalar field with RHS = true
    IdentifierExpression lhs = IdentifierExpression.of("item");
    ConstantExpression rhs = ConstantExpression.of("null");
    RelationalExpression expression = RelationalExpression.of(lhs, NOT_EXISTS, rhs);

    when(lhsParser.visit(any(IdentifierExpression.class))).thenReturn("document->>'item'");

    String result = parser.parse(expression, context);

    assertEquals(
        "document->>'item' IS NULL",
        result,
        "NOT_EXISTS with RHS=true on SCALAR should check IS NULL");
  }

  @Test
  void testParse_jsonScalarField_rhsFalse() {
    // Test NOT_EXISTS on JSON scalar (non-array) field with RHS = false
    JsonIdentifierExpression lhs =
        JsonIdentifierExpression.of("props", JsonFieldType.STRING, "brand");
    ConstantExpression rhs = ConstantExpression.of(false);
    RelationalExpression expression = RelationalExpression.of(lhs, NOT_EXISTS, rhs);

    when(lhsParser.visit(any(JsonIdentifierExpression.class)))
        .thenReturn("document->'props'->>'brand'");

    String result = parser.parse(expression, context);

    assertEquals(
        "document->'props'->>'brand' IS NOT NULL",
        result,
        "NOT_EXISTS with RHS=false on JSON scalar should check IS NOT NULL");
  }
}
