package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser.PostgresRelationalFilterContext;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresTopLevelArrayEqualityFilterParserTest {

  private PostgresTopLevelArrayEqualityFilterParser parser;
  private PostgresRelationalFilterContext context;
  private PostgresSelectTypeExpressionVisitor lhsParser;
  private PostgresQueryParser queryParser;
  private Params.Builder paramsBuilder;

  @BeforeEach
  void setUp() {
    parser = new PostgresTopLevelArrayEqualityFilterParser();
    lhsParser = mock(PostgresSelectTypeExpressionVisitor.class);
    queryParser = mock(PostgresQueryParser.class);
    paramsBuilder = Params.newBuilder();

    when(queryParser.getParamsBuilder()).thenReturn(paramsBuilder);

    context =
        PostgresRelationalFilterContext.builder()
            .lhsParser(lhsParser)
            .postgresQueryParser(queryParser)
            .build();
  }

  @Test
  void testParseNullRhs() {
    ArrayIdentifierExpression lhs = ArrayIdentifierExpression.ofStrings("tags");
    ConstantExpression rhs = ConstantExpression.of("ignored");
    RelationalExpression expression = RelationalExpression.of(lhs, EQ, rhs);

    when(lhsParser.visit(any(ArrayIdentifierExpression.class))).thenReturn("tags");
    // Simulate rhsParser returning null
    PostgresSelectTypeExpressionVisitor rhsParser = mock(PostgresSelectTypeExpressionVisitor.class);
    when(rhsParser.visit(rhs)).thenReturn(null);
    context =
        PostgresRelationalFilterContext.builder()
            .lhsParser(lhsParser)
            .rhsParser(rhsParser)
            .postgresQueryParser(queryParser)
            .build();

    String result = parser.parse(expression, context);

    assertEquals("tags IS NULL", result);
    assertEquals(0, paramsBuilder.build().getObjectParams().size());
  }

  @Test
  void testParseScalarRhsEq() {
    ArrayIdentifierExpression lhs = ArrayIdentifierExpression.ofStrings("tags");
    ConstantExpression rhs = ConstantExpression.of("hygiene");
    RelationalExpression expression = RelationalExpression.of(lhs, EQ, rhs);

    when(lhsParser.visit(any(ArrayIdentifierExpression.class))).thenReturn("tags");

    String result = parser.parse(expression, context);

    assertEquals("tags = ARRAY[?]::text[]", result);
    assertEquals(1, paramsBuilder.build().getObjectParams().size());
    assertEquals("hygiene", paramsBuilder.build().getObjectParams().get(1));
  }

  @Test
  void testParseIterableRhsEq() {
    ArrayIdentifierExpression lhs = ArrayIdentifierExpression.ofStrings("tags");
    ConstantExpression rhs = ConstantExpression.ofStrings(List.of("hygiene", "family-pack"));
    RelationalExpression expression = RelationalExpression.of(lhs, EQ, rhs);

    when(lhsParser.visit(any(ArrayIdentifierExpression.class))).thenReturn("tags");

    String result = parser.parse(expression, context);

    assertEquals("tags = ARRAY[?, ?]::text[]", result);
    assertEquals(2, paramsBuilder.build().getObjectParams().size());
    assertEquals("hygiene", paramsBuilder.build().getObjectParams().get(1));
    assertEquals("family-pack", paramsBuilder.build().getObjectParams().get(2));
  }

  @Test
  void testParseIterableRhsNeq() {
    ArrayIdentifierExpression lhs = ArrayIdentifierExpression.ofStrings("tags");
    ConstantExpression rhs = ConstantExpression.ofStrings(List.of("a", "b"));
    RelationalExpression expression = RelationalExpression.of(lhs, NEQ, rhs);

    when(lhsParser.visit(any(ArrayIdentifierExpression.class))).thenReturn("tags");

    String result = parser.parse(expression, context);

    assertEquals("tags != ARRAY[?, ?]::text[]", result);
    assertEquals(2, paramsBuilder.build().getObjectParams().size());
    assertEquals("a", paramsBuilder.build().getObjectParams().get(1));
    assertEquals("b", paramsBuilder.build().getObjectParams().get(2));
  }
}
