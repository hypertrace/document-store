package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LIKE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.query.Query;

final class MongoRelationalExpressionParser extends MongoExpressionParser {

  private static final ImmutableMap<
          RelationalOperator, BiFunction<String, Object, Map<String, Object>>>
      HANDLERS =
          ImmutableMap
              .<RelationalOperator, BiFunction<String, Object, Map<String, Object>>>builder()
              .put(EQ, Map::of)
              .put(NEQ, handler("ne"))
              .put(GT, handler("gt"))
              .put(LT, handler("lt"))
              .put(GTE, handler("gte"))
              .put(LTE, handler("lte"))
              .put(IN, handler("in"))
              .put(CONTAINS, handler("elemMatch"))
              .put(EXISTS, handler("exists"))
              .put(NOT_EXISTS, handler("exists"))
              .put(LIKE, likeHandler())
              .put(NOT_IN, handler("nin"))
              .build();

  MongoRelationalExpressionParser(Query query) {
    super(query);
  }

  Map<String, Object> parse(final RelationalExpression expression) {
    SelectingExpression lhs = expression.getLhs();
    RelationalOperator operator = expression.getOperator();
    SelectingExpression rhs = expression.getRhs();

    MongoSelectingExpressionParser selectionParser =
        new MongoNonAggregationSelectingExpressionParser(query);
    String key = Objects.toString(lhs.visit(selectionParser));

    // TODO: Might want to parse RHS with $ prefix to support comparison between fields
    Object value = rhs.visit(selectionParser);

    return generateMap(key, value, operator);
  }

  private static Map<String, Object> generateMap(
      final String key, Object value, final RelationalOperator operator) {
    BiFunction<String, Object, Map<String, Object>> handler =
        HANDLERS.getOrDefault(operator, unknownHandler(operator));

    switch (operator) {
      case EXISTS:
        value = true;
        break;

      case NOT_EXISTS:
        value = false;
        break;
    }

    return handler.apply(key, value);
  }

  private static BiFunction<String, Object, Map<String, Object>> handler(final String op) {
    return (key, value) -> Map.of(key, new BasicDBObject("$" + op, value));
  }

  private static BiFunction<String, Object, Map<String, Object>> likeHandler() {
    return (key, value) ->
        // Case-insensitive regex search
        Map.of(key, new BasicDBObject("$regex", value).append("$options", "i"));
  }

  private static BiFunction<String, Object, Map<String, Object>> unknownHandler(
      final RelationalOperator operator) {
    return (key, value) -> {
      throw getUnsupportedOperationException(operator);
    };
  }
}
