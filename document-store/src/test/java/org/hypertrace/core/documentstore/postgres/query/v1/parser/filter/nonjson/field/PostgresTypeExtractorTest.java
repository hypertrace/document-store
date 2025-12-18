package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.junit.jupiter.api.Test;

class PostgresTypeExtractorTest {

  private final PostgresTypeExtractor extractor = PostgresTypeExtractor.arrayType();

  @Test
  void testVisitArrayIdentifierExpression_withType() {
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.ofStrings("tags");
    String result = extractor.visit(expr);
    assertEquals("text[]", result);
  }

  @Test
  void testVisitArrayIdentifierExpression_withIntegerType() {
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.ofInts("numbers");
    String result = extractor.visit(expr);
    assertEquals("int4[]", result);
  }

  @Test
  void testVisitArrayIdentifierExpression_withBooleanType() {
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.ofBooleans("flags");
    String result = extractor.visit(expr);
    assertEquals("bool[]", result);
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
    assertThrows(UnsupportedOperationException.class, () -> extractor.visit(expr));
  }

  @Test
  void testVisitIdentifierExpression_withType() {
    IdentifierExpression expr = IdentifierExpression.ofString("item");
    String result = extractor.visit(expr);
    assertEquals("text[]", result);
  }

  @Test
  void testVisitIdentifierExpression_withoutType() {
    IdentifierExpression expr = IdentifierExpression.of("item");
    String result = extractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitIdentifierExpression_scalarType_withStringType() {
    PostgresTypeExtractor scalarExtractor = PostgresTypeExtractor.scalarType();
    IdentifierExpression expr = IdentifierExpression.ofString("item");
    String result = scalarExtractor.visit(expr);
    assertEquals("text", result);
  }

  @Test
  void testVisitIdentifierExpression_scalarType_withIntType() {
    PostgresTypeExtractor scalarExtractor = PostgresTypeExtractor.scalarType();
    IdentifierExpression expr = IdentifierExpression.ofInt("price");
    String result = scalarExtractor.visit(expr);
    assertEquals("int4", result);
  }

  @Test
  void testVisitIdentifierExpression_scalarType_withoutType() {
    PostgresTypeExtractor scalarExtractor = PostgresTypeExtractor.scalarType();
    IdentifierExpression expr = IdentifierExpression.of("item");
    String result = scalarExtractor.visit(expr);
    assertNull(result);
  }

  @Test
  void testVisitAggregateExpression() {
    AggregateExpression expr =
        AggregateExpression.of(
            org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT,
            IdentifierExpression.of("item"));
    assertThrows(UnsupportedOperationException.class, () -> extractor.visit(expr));
  }

  @Test
  void testVisitConstantExpression() {
    ConstantExpression expr = ConstantExpression.of("test");
    assertThrows(UnsupportedOperationException.class, () -> extractor.visit(expr));
  }

  @Test
  void testVisitDocumentConstantExpression() {
    ConstantExpression.DocumentConstantExpression expr =
        (ConstantExpression.DocumentConstantExpression)
            ConstantExpression.of((org.hypertrace.core.documentstore.Document) null);
    assertThrows(UnsupportedOperationException.class, () -> extractor.visit(expr));
  }

  @Test
  void testVisitFunctionExpression() {
    FunctionExpression expr =
        FunctionExpression.builder()
            .operator(FunctionOperator.LENGTH)
            .operand(IdentifierExpression.of("item"))
            .build();
    assertThrows(UnsupportedOperationException.class, () -> extractor.visit(expr));
  }

  @Test
  void testVisitAliasedIdentifierExpression() {
    AliasedIdentifierExpression expr =
        AliasedIdentifierExpression.builder().name("item").contextAlias("i").build();
    assertThrows(UnsupportedOperationException.class, () -> extractor.visit(expr));
  }
}
