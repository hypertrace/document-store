package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayType;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.junit.jupiter.api.Test;

class PostgresArrayEqualityParserSelectorTest {

  @Test
  void testVisitJsonIdentifierExpression() {
    PostgresArrayEqualityParserSelector selector = new PostgresArrayEqualityParserSelector();
    JsonIdentifierExpression expr =
        JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors");

    Object result = selector.visit(expr);

    assertNotNull(result);
    assertInstanceOf(PostgresJsonArrayEqualityFilterParser.class, result);
  }

  @Test
  void testVisitArrayIdentifierExpression() {
    PostgresArrayEqualityParserSelector selector = new PostgresArrayEqualityParserSelector();
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);

    Object result = selector.visit(expr);

    assertNotNull(result);
    assertInstanceOf(PostgresTopLevelArrayEqualityFilterParser.class, result);
  }

  @Test
  void testVisitIdentifierExpressionFallsBackToStandardParser() {
    PostgresArrayEqualityParserSelector selector = new PostgresArrayEqualityParserSelector();
    IdentifierExpression expr = IdentifierExpression.of("item");

    Object result = selector.visit(expr);

    assertNotNull(result);
    assertInstanceOf(PostgresStandardRelationalFilterParser.class, result);
  }

  @Test
  void testVisitAggregateExpressionFallsBackToStandardParser() {
    PostgresArrayEqualityParserSelector selector = new PostgresArrayEqualityParserSelector();
    AggregateExpression expr =
        AggregateExpression.of(AggregationOperator.COUNT, IdentifierExpression.of("item"));

    Object result = selector.visit(expr);

    assertNotNull(result);
    assertInstanceOf(PostgresStandardRelationalFilterParser.class, result);
  }

  @Test
  void testVisitConstantExpressionFallsBackToStandardParser() {
    PostgresArrayEqualityParserSelector selector = new PostgresArrayEqualityParserSelector();
    ConstantExpression expr = ConstantExpression.of("test");

    Object result = selector.visit(expr);

    assertNotNull(result);
    assertInstanceOf(PostgresStandardRelationalFilterParser.class, result);
  }

  @Test
  void testVisitDocumentConstantExpressionFallsBackToStandardParser() {
    PostgresArrayEqualityParserSelector selector = new PostgresArrayEqualityParserSelector();
    DocumentConstantExpression expr =
        (DocumentConstantExpression)
            ConstantExpression.of((org.hypertrace.core.documentstore.Document) null);

    Object result = selector.visit(expr);

    assertNotNull(result);
    assertInstanceOf(PostgresStandardRelationalFilterParser.class, result);
  }

  @Test
  void testVisitFunctionExpressionFallsBackToStandardParser() {
    PostgresArrayEqualityParserSelector selector = new PostgresArrayEqualityParserSelector();
    FunctionExpression expr =
        FunctionExpression.builder()
            .operator(FunctionOperator.LENGTH)
            .operand(IdentifierExpression.of("item"))
            .build();

    Object result = selector.visit(expr);

    assertNotNull(result);
    assertInstanceOf(PostgresStandardRelationalFilterParser.class, result);
  }

  @Test
  void testVisitAliasedIdentifierExpressionFallsBackToStandardParser() {
    PostgresArrayEqualityParserSelector selector = new PostgresArrayEqualityParserSelector();
    AliasedIdentifierExpression expr =
        AliasedIdentifierExpression.builder().name("item").contextAlias("i").build();

    Object result = selector.visit(expr);

    assertNotNull(result);
    assertInstanceOf(PostgresStandardRelationalFilterParser.class, result);
  }
}
