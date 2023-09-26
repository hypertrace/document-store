package org.hypertrace.core.documentstore.mongo.query.parser;

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
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.STARTS_WITH;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

final class MongoRelationalExpressionParser {

  private static final Map<
          RelationalOperator,
          BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>>
      HANDLERS =
          unmodifiableMap(
              new EnumMap<>(RelationalOperator.class) {
                {
                  put(EQ, new MongoRelationalExprFilterOperation("eq"));
                  put(NEQ, new MongoRelationalExprFilterOperation("ne"));
                  put(GT, new MongoRelationalExprFilterOperation("gt"));
                  put(LT, new MongoRelationalExprFilterOperation("lt"));
                  put(GTE, new MongoRelationalExprFilterOperation("gte"));
                  put(LTE, new MongoRelationalExprFilterOperation("lte"));
                  put(IN, new MongoRelationalFilterOperation("in"));
                  put(NOT_IN, new MongoRelationalFilterOperation("nin"));
                  put(CONTAINS, new MongoContainsFilterOperation());
                  put(NOT_CONTAINS, new MongoNotContainsFilterOperation());
                  put(EXISTS, new MongoRelationalFilterOperation("exists"));
                  put(NOT_EXISTS, new MongoRelationalFilterOperation("exists"));
                  put(LIKE, new MongoLikeFilterOperation());
                  put(STARTS_WITH, new MongoStartsWithFilterOperation());
                }
              });

  Map<String, Object> parse(final RelationalExpression expression) {
    final SelectTypeExpression lhs = expression.getLhs();
    final RelationalOperator operator = expression.getOperator();
    final SelectTypeExpression rhs = expression.getRhs();
    return generateMap(lhs, rhs, operator);
  }

  private static Map<String, Object> generateMap(
      final SelectTypeExpression lhs, SelectTypeExpression rhs, final RelationalOperator operator) {
    BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>> handler =
        HANDLERS.getOrDefault(operator, unknownHandler(operator));

    switch (operator) {
      case EXISTS:
        rhs = ConstantExpression.of(true);
        break;

      case NOT_EXISTS:
        rhs = ConstantExpression.of(false);
        break;
    }

    return handler.apply(lhs, rhs);
  }

  private static BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>
      unknownHandler(final RelationalOperator operator) {
    return (lhs, rhs) -> {
      throw getUnsupportedOperationException(operator);
    };
  }
}
