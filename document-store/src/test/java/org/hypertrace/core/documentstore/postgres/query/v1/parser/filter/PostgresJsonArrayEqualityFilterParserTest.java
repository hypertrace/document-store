package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser.PostgresRelationalFilterContext;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresJsonArrayEqualityFilterParserTest {

  private PostgresJsonArrayEqualityFilterParser parser;
  private PostgresRelationalFilterContext context;
  private PostgresSelectTypeExpressionVisitor lhsParser;
  private PostgresQueryParser queryParser;
  private Params.Builder paramsBuilder;

  @BeforeEach
  void setUp() {
    parser = new PostgresJsonArrayEqualityFilterParser();
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
  void testParseWithNullRHS() {
    JsonIdentifierExpression lhs =
        JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors");
    // RHS parsed value will be null from rhsParser
    ConstantExpression rhs = ConstantExpression.of("ignored");
    RelationalExpression expression = RelationalExpression.of(lhs, EQ, rhs);

    when(lhsParser.visit(any(JsonIdentifierExpression.class))).thenReturn("props->'colors'");
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

    assertEquals("props->'colors' IS NULL", result);
    assertEquals(0, paramsBuilder.build().getObjectParams().size());
  }

  @Test
  void testParseIterableRhsEq() {
    JsonIdentifierExpression lhs =
        JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors");
    ConstantExpression rhs = ConstantExpression.ofStrings(List.of("Blue", "Green"));
    RelationalExpression expression = RelationalExpression.of(lhs, EQ, rhs);

    when(lhsParser.visit(any(JsonIdentifierExpression.class))).thenReturn("props->'colors'");

    String result = parser.parse(expression, context);

    assertEquals("props->'colors' = ?::jsonb", result);
    assertEquals(1, paramsBuilder.build().getObjectParams().size());
    assertEquals("[\"Blue\",\"Green\"]", paramsBuilder.build().getObjectParams().get(1));
  }

  /** Tests parsing when RHS is an iterable (e.g., list of strings) and the operator is NEQ */
  @Test
  void testParseIterableRhsNeq() {
    JsonIdentifierExpression lhs =
        JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors");
    ConstantExpression rhs = ConstantExpression.ofStrings(List.of("Blue", "Red"));
    RelationalExpression expression = RelationalExpression.of(lhs, NEQ, rhs);

    when(lhsParser.visit(any(JsonIdentifierExpression.class))).thenReturn("props->'colors'");

    String result = parser.parse(expression, context);

    assertEquals("props->'colors' != ?::jsonb", result);
    assertEquals(1, paramsBuilder.build().getObjectParams().size());
    assertEquals("[\"Blue\",\"Red\"]", paramsBuilder.build().getObjectParams().get(1));
  }
}
