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
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresContainsRelationalFilterParserNonJsonField;
import org.junit.jupiter.api.Test;

class PostgresContainsParserSelectorTest {

  @Test
  void testVisitArrayIdentifierExpression_flatCollection() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(true);
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresContainsRelationalFilterParserNonJsonField.class, result);
  }

  @Test
  void testVisitArrayIdentifierExpression_nestedCollection() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(false);
    ArrayIdentifierExpression expr = ArrayIdentifierExpression.of("tags", ArrayType.TEXT);
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresContainsRelationalFilterParser.class, result);
  }

  @Test
  void testVisitJsonIdentifierExpression() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(true);
    JsonIdentifierExpression expr = JsonIdentifierExpression.of("customAttr", "field");
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresContainsRelationalFilterParser.class, result);
  }

  @Test
  void testVisitIdentifierExpression_flatCollection() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(true);
    IdentifierExpression expr = IdentifierExpression.of("item");
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresContainsRelationalFilterParserNonJsonField.class, result);
  }

  @Test
  void testVisitIdentifierExpression_nestedCollection() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(false);
    IdentifierExpression expr = IdentifierExpression.of("item");
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
    assertInstanceOf(PostgresContainsRelationalFilterParser.class, result);
  }

  @Test
  void testVisitAggregateExpression_flatCollection() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(true);
    AggregateExpression expr =
        AggregateExpression.of(AggregationOperator.COUNT, IdentifierExpression.of("item"));
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
  }

  @Test
  void testVisitConstantExpression() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(true);
    ConstantExpression expr = ConstantExpression.of("test");
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
  }

  @Test
  void testVisitDocumentConstantExpression() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(true);
    ConstantExpression.DocumentConstantExpression expr =
        (ConstantExpression.DocumentConstantExpression)
            ConstantExpression.of((org.hypertrace.core.documentstore.Document) null);
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
  }

  @Test
  void testVisitFunctionExpression() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(true);
    FunctionExpression expr =
        FunctionExpression.builder()
            .operator(FunctionOperator.LENGTH)
            .operand(IdentifierExpression.of("item"))
            .build();
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
  }

  @Test
  void testVisitAliasedIdentifierExpression() {
    PostgresContainsParserSelector selector = new PostgresContainsParserSelector(true);
    AliasedIdentifierExpression expr =
        AliasedIdentifierExpression.builder().name("item").contextAlias("i").build();
    PostgresRelationalFilterParser result = selector.visit(expr);
    assertNotNull(result);
  }
}
