package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayType;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserArrayField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserScalarField;
import org.junit.jupiter.api.Test;

class PostgresInParserSelectorTest {

  @Test
  void testVisitArrayIdentifierExpression_flatCollection() {
    PostgresInParserSelector selector = new PostgresInParserSelector(true);
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresInRelationalFilterParserArrayField.class, result);
  }

  @Test
  void testVisitArrayIdentifierExpression_nestedCollection() {
    PostgresInParserSelector selector = new PostgresInParserSelector(false);
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresInRelationalFilterParser.class, result);
  }

  @Test
  void testVisitJsonIdentifierExpression() {
    PostgresInParserSelector selector = new PostgresInParserSelector(true);
    JsonIdentifierExpression expr = JsonIdentifierExpression.of("customAttr", "field");
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresInRelationalFilterParser.class, result);
  }

  @Test
  void testVisitIdentifierExpression_flatCollection() {
    PostgresInParserSelector selector = new PostgresInParserSelector(true);
    IdentifierExpression expr = IdentifierExpression.of("item");
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresInRelationalFilterParserScalarField.class, result);
  }

  @Test
  void testVisitIdentifierExpression_nestedCollection() {
    PostgresInParserSelector selector = new PostgresInParserSelector(false);
    IdentifierExpression expr = IdentifierExpression.of("item");
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresInRelationalFilterParser.class, result);
  }

  @Test
  void testVisitAggregateExpression_flatCollection() {
    PostgresInParserSelector selector = new PostgresInParserSelector(true);
    AggregateExpression expr =
        AggregateExpression.of(AggregationOperator.COUNT, IdentifierExpression.of("item"));
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
  }

  @Test
  void testVisitConstantExpression() {
    PostgresInParserSelector selector = new PostgresInParserSelector(true);
    ConstantExpression expr = ConstantExpression.of("test");
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
  }

  @Test
  void testVisitDocumentConstantExpression() {
    PostgresInParserSelector selector = new PostgresInParserSelector(true);
    ConstantExpression.DocumentConstantExpression expr =
        (ConstantExpression.DocumentConstantExpression)
            ConstantExpression.of((org.hypertrace.core.documentstore.Document) null);
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
  }

  @Test
  void testVisitFunctionExpression() {
    PostgresInParserSelector selector = new PostgresInParserSelector(true);
    FunctionExpression expr =
        FunctionExpression.builder()
            .operator(FunctionOperator.LENGTH)
            .operand(IdentifierExpression.of("item"))
            .build();
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
  }

  @Test
  void testVisitAliasedIdentifierExpression() {
    PostgresInParserSelector selector = new PostgresInParserSelector(true);
    AliasedIdentifierExpression expr =
        AliasedIdentifierExpression.builder().name("item").contextAlias("i").build();
    PostgresInRelationalFilterParserInterface result = selector.visit(expr);
    assertNotNull(result);
  }
}
