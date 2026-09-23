package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.DIVIDE;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.FLOOR;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

class MongoFunctionGroupByTest {

  @Test
  void groupsByAliasedArithmeticFunction() {
    IdentifierExpression timestamp =
        IdentifierExpression.of("attributes.last_activity_timestamp.value.long");
    ConstantExpression interval = ConstantExpression.of(86_400_000L);
    FunctionExpression bucket =
        FunctionExpression.builder()
            .alias("INTERVAL_START_TIME")
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
}
