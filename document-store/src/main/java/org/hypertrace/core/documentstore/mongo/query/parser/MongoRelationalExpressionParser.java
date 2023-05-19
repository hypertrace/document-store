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
import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import com.mongodb.BasicDBObject;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MongoRelationalExpressionParser {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MongoRelationalExpressionParser.class);

  private static final String EXPR = "$expr";
  private static final String REGEX = "$regex";
  private static final String OPTIONS = "$options";
  private static final String IGNORE_CASE_OPTION = "i";

  private static final Map<
          RelationalOperator,
          BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>>
      HANDLERS =
          unmodifiableMap(
              new EnumMap<>(RelationalOperator.class) {
                {
                  put(EQ, expressionHandler("eq"));
                  put(NEQ, expressionHandler("ne"));
                  put(GT, expressionHandler("gt"));
                  put(LT, expressionHandler("lt"));
                  put(GTE, expressionHandler("gte"));
                  put(LTE, expressionHandler("lte"));
                  put(IN, handler("in"));
                  put(CONTAINS, handler("elemMatch"));
                  put(NOT_CONTAINS, notContainsHandler());
                  put(EXISTS, handler("exists"));
                  put(NOT_EXISTS, handler("exists"));
                  put(LIKE, likeHandler());
                  put(NOT_IN, handler("nin"));
                }
              });

  private static final MongoSelectTypeExpressionParser functionParser =
      new MongoFunctionExpressionParser();
  private static final MongoSelectTypeExpressionParser identifierParser =
      new MongoIdentifierExpressionParser();
  // Only a constant RHS is supported as of now
  private static final MongoSelectTypeExpressionParser rhsParser =
      new MongoConstantExpressionParser();

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
      handler(final String op) {
    return (lhs, rhs) -> {
      final String parsedLhs = lhs.accept(identifierParser);
      final Object parsedRhs = rhs.accept(rhsParser);
      return Map.of(parsedLhs, new BasicDBObject(PREFIX + op, parsedRhs));
    };
  }

  private static BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>
      expressionHandler(final String op) {
    return (lhs, rhs) -> {
      // Use $expr type expression for FunctionExpression with normal handler as a fallback
      try {
        final Object parsedLhs = lhs.accept(functionParser);
        final Object parsedRhs = rhs.accept(rhsParser);
        return Map.of(EXPR, new BasicDBObject(PREFIX + op, new Object[] {parsedLhs, parsedRhs}));
      } catch (final UnsupportedOperationException e) {
        return handler(op).apply(lhs, rhs);
      }
    };
  }

  private static BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>
      likeHandler() {
    return (lhs, rhs) -> {
      final String parsedLhs = lhs.accept(identifierParser);
      final Object parsedRhs = rhs.accept(rhsParser);
      return Map.of(
          parsedLhs, new BasicDBObject(REGEX, parsedRhs).append(OPTIONS, IGNORE_CASE_OPTION));
    };
  }

  private static BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>
      unknownHandler(final RelationalOperator operator) {
    return (lhs, rhs) -> {
      throw getUnsupportedOperationException(operator);
    };
  }

  private static BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>
      notContainsHandler() {
    return (lhs, rhs) -> {
      final String parsedLhs = lhs.accept(identifierParser);
      final Object parsedRhs = rhs.accept(rhsParser);
      return Map.of(
          parsedLhs, new BasicDBObject("$not", new BasicDBObject(PREFIX + "elemMatch", parsedRhs)));
    };
  }
}
