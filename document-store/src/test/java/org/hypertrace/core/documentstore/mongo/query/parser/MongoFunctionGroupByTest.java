package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.DIVIDE;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.FLOOR;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.BasicDBObject;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class MongoFunctionGroupByTest {

  @Test
  void groupsByAliasedArithmeticFunction() {
    FunctionExpression bucket = bucket("INTERVAL_START_TIME");

    Query query =
        Query.builder()
            .addSelection(bucket, "INTERVAL_START_TIME")
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("id")), "count")
            .addAggregation(bucket)
            .addAggregation(IdentifierExpression.of("attributes.score_category"))
            .build();

    List<BasicDBObject> clauses = MongoGroupTypeExpressionParser.getGroupClauses(query);
    assertEquals(2, clauses.size());
    assertTrue(clauses.get(0).containsKey("$addFields"));
    assertTrue(clauses.get(1).containsKey("$group"));

    Map<?, ?> addFields = (Map<?, ?>) clauses.get(0).get("$addFields");
    assertTrue(addFields.containsKey("INTERVAL_START_TIME"));

    Map<?, ?> group = (Map<?, ?>) clauses.get(1).get("$group");
    Map<?, ?> id = (Map<?, ?>) group.get("_id");
    assertEquals("$INTERVAL_START_TIME", id.get("INTERVAL_START_TIME"));
    assertEquals("$attributes.score_category", id.get("attributes\\u002escore_category"));

    BasicDBObject projection = MongoSelectTypeExpressionParser.getSelections(query);
    assertEquals("$_id.INTERVAL_START_TIME", projection.get("INTERVAL_START_TIME"));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", " "})
  void rejectsFunctionGroupByWhenAliasIsMissing(final String alias) {
    FunctionExpression bucket = bucket(alias);
    Query query =
        Query.builder()
            .addSelection(bucket, "foo")
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("id")), "count")
            .addAggregation(bucket)
            .build();

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> MongoGroupTypeExpressionParser.getGroupClauses(query));
    assertTrue(exception.getMessage().contains("not yet supported"));
  }

  @Test
  void projectsSelectionAliasWhenItDiffersFromFunctionAlias() {
    FunctionExpression bucket = bucket("bar");
    Query query =
        Query.builder()
            .addSelection(bucket, "foo")
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("id")), "count")
            .addAggregation(bucket)
            .build();

    List<BasicDBObject> clauses = MongoGroupTypeExpressionParser.getGroupClauses(query);
    assertEquals(2, clauses.size());

    Map<?, ?> addFields = (Map<?, ?>) clauses.get(0).get("$addFields");
    assertTrue(addFields.containsKey("bar"));
    assertFalse(addFields.containsKey("foo"));

    Map<?, ?> group = (Map<?, ?>) clauses.get(1).get("$group");
    Map<?, ?> id = (Map<?, ?>) group.get("_id");
    assertEquals("$bar", id.get("bar"));
    assertFalse(id.containsKey("foo"));

    BasicDBObject projection = MongoSelectTypeExpressionParser.getSelections(query);
    assertEquals("$_id.bar", projection.get("foo"));
    assertFalse(projection.containsKey("bar"));
  }

  private static FunctionExpression bucket(final String alias) {
    IdentifierExpression timestamp =
        IdentifierExpression.of("attributes.last_activity_timestamp.value.long");
    ConstantExpression interval = ConstantExpression.of(86_400_000L);
    return FunctionExpression.builder()
        .alias(alias)
        .operator(MULTIPLY)
        .operand(
            FunctionExpression.builder()
                .operator(FLOOR)
                .operand(
                    FunctionExpression.builder()
                        .operator(DIVIDE)
                        .operand(timestamp)
                        .operand(interval)
                        .build())
                .build())
        .operand(interval)
        .build();
  }
}
