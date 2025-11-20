package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayType;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.junit.jupiter.api.Test;

class PostgresArrayTypeExtractorTest {

  private final PostgresArrayTypeExtractor extractor = new PostgresArrayTypeExtractor();

  @Test
  void testVisitArrayIdentifierExpression_withType() {
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);
    String result = extractor.visit(expr);
    assertEquals("text[]", result);
  }

  @Test
  void testVisitArrayIdentifierExpression_withIntegerType() {
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("numbers", ArrayType.INTEGER);
    String result = extractor.visit(expr);
    assertEquals("integer[]", result);
  }

  @Test
  void testVisitArrayIdentifierExpression_withBooleanType() {
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("flags", ArrayType.BOOLEAN);
    String result = extractor.visit(expr);
    assertEquals("boolean[]", result);
  }

  @Test
  void testVisitArrayIdentifierExpression_withoutType() {
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("tags");
    String result = extractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitJsonIdentifierExpression() {
    JsonIdentifierExpression expr = JsonIdentifierExpression.of("customAttr", "field");
    String result = extractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitIdentifierExpression() {
    IdentifierExpression expr = IdentifierExpression.of("item");
    String result = extractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitAggregateExpression() {
    AggregateExpression expr =
        AggregateExpression.of(
            org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT,
            IdentifierExpression.of("item"));
    String result = extractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitConstantExpression() {
    ConstantExpression expr = ConstantExpression.of("test");
    String result = extractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitDocumentConstantExpression() {
    ConstantExpression.DocumentConstantExpression expr =
        (ConstantExpression.DocumentConstantExpression)
            ConstantExpression.of((org.hypertrace.core.documentstore.Document) null);
    String result = extractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitFunctionExpression() {
    FunctionExpression expr =
        FunctionExpression.builder()
            .operator(FunctionOperator.LENGTH)
            .operand(IdentifierExpression.of("item"))
            .build();
    String result = extractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitAliasedIdentifierExpression() {
    AliasedIdentifierExpression expr =
        AliasedIdentifierExpression.builder().name("item").contextAlias("i").build();
    String result = extractor.visit(expr);
    assertNull(result);
  }
}
