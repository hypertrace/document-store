package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayType;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresFieldTypeDetector.FieldCategory;
import org.junit.jupiter.api.Test;

class PostgresFieldTypeDetectorTest {

  private final PostgresFieldTypeDetector detector = new PostgresFieldTypeDetector();

  @Test
  void testVisitArrayIdentifierExpression_returnsArray() {
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(FieldCategory.ARRAY, result, "ArrayIdentifierExpression should return ARRAY");
  }

  @Test
  void testVisitJsonIdentifierExpression_stringArray_returnsJsonbArray() {
    JsonIdentifierExpression expr =
        JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors");
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(
        FieldCategory.JSONB_ARRAY,
        result,
        "JsonIdentifierExpression with STRING_ARRAY should return JSONB_ARRAY");
  }

  @Test
  void testVisitJsonIdentifierExpression_numberArray_returnsJsonbArray() {
    JsonIdentifierExpression expr =
        JsonIdentifierExpression.of("props", JsonFieldType.NUMBER_ARRAY, "scores");
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(
        FieldCategory.JSONB_ARRAY,
        result,
        "JsonIdentifierExpression with NUMBER_ARRAY should return JSONB_ARRAY");
  }

  @Test
  void testVisitJsonIdentifierExpression_booleanArray_returnsJsonbArray() {
    JsonIdentifierExpression expr =
        JsonIdentifierExpression.of("props", JsonFieldType.BOOLEAN_ARRAY, "flags");
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(
        FieldCategory.JSONB_ARRAY,
        result,
        "JsonIdentifierExpression with BOOLEAN_ARRAY should return JSONB_ARRAY");
  }

  @Test
  void testVisitJsonIdentifierExpression_objectArray_returnsJsonbArray() {
    JsonIdentifierExpression expr =
        JsonIdentifierExpression.of("props", JsonFieldType.OBJECT_ARRAY, "items");
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(
        FieldCategory.JSONB_ARRAY,
        result,
        "JsonIdentifierExpression with OBJECT_ARRAY should return JSONB_ARRAY");
  }

  @Test
  void testVisitJsonIdentifierExpression_stringScalar_returnsScalar() {
    JsonIdentifierExpression expr =
        JsonIdentifierExpression.of("props", JsonFieldType.STRING, "brand");
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(
        FieldCategory.JSONB_SCALAR, result,
        "JsonIdentifierExpression with STRING should return SCALAR");
  }

  @Test
  void testVisitJsonIdentifierExpression_noFieldType_returnsScalar() {
    JsonIdentifierExpression expr = JsonIdentifierExpression.of("props", "field");
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(
        FieldCategory.JSONB_SCALAR,
        result,
        "JsonIdentifierExpression without field type should return SCALAR");
  }

  @Test
  void testVisitIdentifierExpression_returnsScalar() {
    IdentifierExpression expr = IdentifierExpression.of("item");
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(FieldCategory.SCALAR, result, "IdentifierExpression should return SCALAR");
  }

  @Test
  void testVisitAggregateExpression_returnsScalar() {
    AggregateExpression expr =
        AggregateExpression.of(AggregationOperator.COUNT, IdentifierExpression.of("item"));
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(FieldCategory.SCALAR, result, "AggregateExpression should return SCALAR");
  }

  @Test
  void testVisitConstantExpression_returnsScalar() {
    ConstantExpression expr = ConstantExpression.of("test");
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(FieldCategory.SCALAR, result, "ConstantExpression should return SCALAR");
  }

  @Test
  void testVisitDocumentConstantExpression_returnsScalar() {
    ConstantExpression.DocumentConstantExpression expr =
        (ConstantExpression.DocumentConstantExpression)
            ConstantExpression.of((org.hypertrace.core.documentstore.Document) null);
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(FieldCategory.SCALAR, result, "DocumentConstantExpression should return SCALAR");
  }

  @Test
  void testVisitFunctionExpression_returnsScalar() {
    FunctionExpression expr =
        FunctionExpression.builder()
            .operator(FunctionOperator.LENGTH)
            .operand(IdentifierExpression.of("item"))
            .build();
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(FieldCategory.SCALAR, result, "FunctionExpression should return SCALAR");
  }

  @Test
  void testVisitAliasedIdentifierExpression_returnsScalar() {
    AliasedIdentifierExpression expr =
        AliasedIdentifierExpression.builder().name("item").contextAlias("i").build();
    FieldCategory result = detector.visit(expr);
    assertNotNull(result);
    assertEquals(FieldCategory.SCALAR, result, "AliasedIdentifierExpression should return SCALAR");
  }
}
