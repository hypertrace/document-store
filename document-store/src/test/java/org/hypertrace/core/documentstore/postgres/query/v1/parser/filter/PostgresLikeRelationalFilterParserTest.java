package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser.PostgresRelationalFilterContext;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresLikeRelationalFilterParserTest {

  private PostgresLikeRelationalFilterParser parser;
  private PostgresRelationalFilterContext context;
  private PostgresSelectTypeExpressionVisitor lhsParser;
  private PostgresSelectTypeExpressionVisitor rhsParser;
  private Params.Builder paramsBuilder;

  @BeforeEach
  void setUp() {
    parser = new PostgresLikeRelationalFilterParser();
    context = mock(PostgresRelationalFilterContext.class);
    lhsParser = mock(PostgresSelectTypeExpressionVisitor.class);
    rhsParser = mock(PostgresSelectTypeExpressionVisitor.class);
    paramsBuilder = Params.newBuilder();

    when(context.lhsParser()).thenReturn(lhsParser);
    when(context.rhsParser()).thenReturn(rhsParser);
    when(context.getParamsBuilder()).thenReturn(paramsBuilder);
  }

  @Test
  void testParse_plainValue() {
    IdentifierExpression lhs = IdentifierExpression.of("key1");
    ConstantExpression rhs = ConstantExpression.of("abc");
    RelationalExpression expression = RelationalExpression.of(lhs, RelationalOperator.LIKE, rhs);

    when(lhsParser.visit(any(IdentifierExpression.class))).thenReturn("document->>'key1'");
    when(rhsParser.visit(any(ConstantExpression.class))).thenReturn("abc");

    String result = parser.parse(expression, context);

    assertEquals("document->>'key1' ~* ?", result);
    assertEquals(Map.of(1, "abc"), paramsBuilder.build().getObjectParams());
  }

  @Test
  void testParse_valueWithRegexMetacharacters() {
    // POSIX ARE metacharacters (e.g. "{{") must be escaped so Postgres's strict ~* operator
    // matches the value literally instead of throwing "invalid regular expression: quantifier
    // operand invalid".
    IdentifierExpression lhs = IdentifierExpression.of("key1");
    ConstantExpression rhs = ConstantExpression.of("a{{b");
    RelationalExpression expression = RelationalExpression.of(lhs, RelationalOperator.LIKE, rhs);

    when(lhsParser.visit(any(IdentifierExpression.class))).thenReturn("document->>'key1'");
    when(rhsParser.visit(any(ConstantExpression.class))).thenReturn("a{{b");

    String result = parser.parse(expression, context);

    assertEquals("document->>'key1' ~* ?", result);
    assertEquals(Map.of(1, "a\\{\\{b"), paramsBuilder.build().getObjectParams());
  }

  @Test
  void testParse_valueWithAllMetacharacters() {
    // "${${8gs" is not a valid Java/Postgres regex (unmatched quantifier brace), so it must be
    // escaped and matched literally, same as "a{{b" above.
    IdentifierExpression lhs = IdentifierExpression.of("key1");
    ConstantExpression rhs = ConstantExpression.of("${${8gs");
    RelationalExpression expression = RelationalExpression.of(lhs, RelationalOperator.LIKE, rhs);

    when(lhsParser.visit(any(IdentifierExpression.class))).thenReturn("document->>'key1'");
    when(rhsParser.visit(any(ConstantExpression.class))).thenReturn("${${8gs");

    String result = parser.parse(expression, context);

    assertEquals("document->>'key1' ~* ?", result);
    assertEquals(Map.of(1, "\\$\\{\\$\\{8gs"), paramsBuilder.build().getObjectParams());
  }

  @Test
  void testParse_valueIsValidRegex_passedThroughUnchanged() {
    // A value that is already a valid regex must be passed through unchanged rather than
    // escaped, since LIKE is a case-insensitive regex match (not literal "contains").
    IdentifierExpression lhs = IdentifierExpression.of("key1");
    ConstantExpression rhs = ConstantExpression.of(".*amp.*");
    RelationalExpression expression = RelationalExpression.of(lhs, RelationalOperator.LIKE, rhs);

    when(lhsParser.visit(any(IdentifierExpression.class))).thenReturn("document->>'key1'");
    when(rhsParser.visit(any(ConstantExpression.class))).thenReturn(".*amp.*");

    String result = parser.parse(expression, context);

    assertEquals("document->>'key1' ~* ?", result);
    assertEquals(Map.of(1, ".*amp.*"), paramsBuilder.build().getObjectParams());
  }

  @Test
  void testParse_valueIsValidAnchoredRegex_passedThroughUnchanged() {
    IdentifierExpression lhs = IdentifierExpression.of("key1");
    ConstantExpression rhs = ConstantExpression.of("^Sun");
    RelationalExpression expression = RelationalExpression.of(lhs, RelationalOperator.LIKE, rhs);

    when(lhsParser.visit(any(IdentifierExpression.class))).thenReturn("document->>'key1'");
    when(rhsParser.visit(any(ConstantExpression.class))).thenReturn("^Sun");

    String result = parser.parse(expression, context);

    assertEquals("document->>'key1' ~* ?", result);
    assertEquals(Map.of(1, "^Sun"), paramsBuilder.build().getObjectParams());
  }
}
