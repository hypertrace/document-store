package org.hypertrace.core.documentstore.mongo.query.parser;

import static java.util.Map.entry;
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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.function.BiFunction;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

final class MongoRelationalExpressionParser {

  private final MongoSelectTypeExpressionParser lhsParser = new MongoIdentifierExpressionParser();

  // Only a constant RHS is supported for now
  private final MongoSelectTypeExpressionParser rhsParser = new MongoConstantExpressionParser();

  private final Map<RelationalOperator, RelationalFilterOperation> HANDLERS =
      Maps.immutableEnumMap(
          Map.ofEntries(
              entry(EQ, new MongoFunctionRelationalFilterOperation(lhsParser, rhsParser, "$eq")),
              entry(NEQ, new MongoFunctionRelationalFilterOperation(lhsParser, rhsParser, "$ne")),
              entry(GT, new MongoFunctionRelationalFilterOperation(lhsParser, rhsParser, "$gt")),
              entry(LT, new MongoFunctionRelationalFilterOperation(lhsParser, rhsParser, "$lt")),
              entry(GTE, new MongoFunctionRelationalFilterOperation(lhsParser, rhsParser, "$gte")),
              entry(LTE, new MongoFunctionRelationalFilterOperation(lhsParser, rhsParser, "$lte")),
              entry(IN, new MongoRelationalFilterOperation(lhsParser, rhsParser, "$in")),
              entry(NOT_IN, new MongoRelationalFilterOperation(lhsParser, rhsParser, "$nin")),
              entry(CONTAINS, new MongoContainsFilterOperation(lhsParser, rhsParser)),
              entry(NOT_CONTAINS, new MongoNotContainsFilterOperation(lhsParser, rhsParser)),
              entry(EXISTS, new MongoRelationalFilterOperation(lhsParser, rhsParser, "$exists")),
              entry(
                  NOT_EXISTS, new MongoRelationalFilterOperation(lhsParser, rhsParser, "$exists")),
              entry(LIKE, new MongoLikeFilterOperation(lhsParser, rhsParser)),
              entry(STARTS_WITH, new MongoStartsWithFilterOperation(lhsParser, rhsParser))));

  Map<String, Object> parse(final RelationalExpression expression) {
    final SelectTypeExpression lhs = expression.getLhs();
    final RelationalOperator operator = expression.getOperator();
    final SelectTypeExpression rhs = expression.getRhs();
    return generateMap(lhs, rhs, operator);
  }

  private Map<String, Object> generateMap(
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

  private RelationalFilterOperation unknownHandler(final RelationalOperator operator) {
    return (lhs, rhs) -> {
      throw getUnsupportedOperationException(operator);
    };
  }
}
