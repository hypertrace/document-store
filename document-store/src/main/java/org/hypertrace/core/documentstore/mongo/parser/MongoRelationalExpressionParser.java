package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.LIKE;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.NOT_EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationOperator.NOT_IN;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import com.mongodb.BasicDBObject;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.hypertrace.core.documentstore.expression.impl.RelationExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

final class MongoRelationalExpressionParser {

  private static final Map<RelationOperator, BiFunction<String, Object, Map<String, Object>>>
      HANDLERS =
          unmodifiableMap(
              new EnumMap<>(RelationOperator.class) {
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

  Map<String, Object> parse(final RelationExpression expression) {
    SelectTypeExpression lhs = expression.getLhs();
    RelationOperator operator = expression.getOperator();
    SelectTypeExpression rhs = expression.getRhs();

    // Only an identifier LHS and a constant RHS is supported as of now.
    MongoSelectTypeExpressionParser lhsParser = new MongoIdentifierExpressionParser();
    MongoSelectTypeExpressionParser rhsParser = new MongoConstantExpressionParser();

    String key = lhs.accept(lhsParser);
    Object value = rhs.accept(rhsParser);

    return generateMap(key, value, operator);
  }

  private static Map<String, Object> generateMap(
      final String key, Object value, final RelationOperator operator) {
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
      final RelationOperator operator) {
    return (key, value) -> {
      throw getUnsupportedOperationException(operator);
    };
  }
}
