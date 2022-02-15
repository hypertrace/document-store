package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
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
import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import com.mongodb.BasicDBObject;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

final class MongoRelationalExpressionParser {

  private static final Map<RelationalOperator, BiFunction<String, Object, Map<String, Object>>>
      HANDLERS =
          unmodifiableMap(
              new EnumMap<>(RelationalOperator.class) {
                {
                  put(EQ, Map::of);
                  put(NEQ, handler("ne"));
                  put(GT, handler("gt"));
                  put(LT, handler("lt"));
                  put(GTE, handler("gte"));
                  put(LTE, handler("lte"));
                  put(IN, handler("in"));
                  put(CONTAINS, handler("elemMatch"));
                  put(EXISTS, handler("exists"));
                  put(NOT_EXISTS, handler("exists"));
                  put(LIKE, likeHandler());
                  put(NOT_IN, handler("nin"));
                }
              });

  Map<String, Object> parse(final RelationalExpression expression) {
    SelectTypeExpression lhs = expression.getLhs();
    RelationalOperator operator = expression.getOperator();
    SelectTypeExpression rhs = expression.getRhs();

    // Only an identifier LHS and a constant RHS is supported as of now.
    MongoSelectTypeExpressionParser lhsParser = new MongoIdentifierExpressionParser();
    MongoSelectTypeExpressionParser rhsParser = new MongoConstantExpressionParser();

    String key = lhs.accept(lhsParser);
    Object value = rhs.accept(rhsParser);

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
    return (key, value) -> Map.of(key, new BasicDBObject(PREFIX + op, value));
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
