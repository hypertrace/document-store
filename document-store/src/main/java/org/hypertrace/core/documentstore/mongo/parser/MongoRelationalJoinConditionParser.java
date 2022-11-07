package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import com.mongodb.BasicDBObject;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.mongo.parser.MongoJoinConditionParser.MongoJoinParseResult;

final class MongoRelationalJoinConditionParser {
  private static final String EXPR = "$expr";

  private final Map<
          RelationalOperator,
          BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>>
      HANDLERS =
          unmodifiableMap(
              new EnumMap<>(RelationalOperator.class) {
                {
                  put(EQ, expressionHandler("eq"));
                }
              });

  private final MongoIdentifierCollectingParser lhsParser =
      new MongoIdentifierCollectingParser(
          new MongoFunctionExpressionParser(
              new MongoSubQueryIdentifierPrefixingParser(
                  new MongoSubQueryIdentifierExpressionParser()),
              new MongoIdentifierPrefixingParser(
                  new MongoIdentifierPrefixingParser(
                      new MongoIdentifierExpressionParser(
                          new MongoSubQueryIdentifierPrefixingParser(
                              new MongoSubQueryIdentifierExpressionParser(
                                  new MongoConstantExpressionParser())))))));
  private final MongoIdentifierCollectingParser rhsParser =
      new MongoIdentifierCollectingParser(
          new MongoIdentifierPrefixingParser(
              new MongoIdentifierPrefixingParser(
                  new MongoIdentifierExpressionParser(new MongoConstantExpressionParser()))));
  private final Set<String> identifiers = new HashSet<>();

  MongoJoinParseResult parse(final RelationalExpression expression) {
    final SelectTypeExpression lhs = expression.getLhs();
    final RelationalOperator operator = expression.getOperator();
    final SelectTypeExpression rhs = expression.getRhs();
    final Map<String, Object> parsedFilter = generateMap(lhs, rhs, operator);
    return MongoJoinParseResult.builder().parsedFilter(parsedFilter).variables(identifiers).build();
  }

  private Map<String, Object> generateMap(
      final SelectTypeExpression lhs, SelectTypeExpression rhs, final RelationalOperator operator) {
    BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>> handler =
        HANDLERS.getOrDefault(operator, unknownHandler(operator));
    return handler.apply(lhs, rhs);
  }

  @SuppressWarnings("SameParameterValue")
  private BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>
      expressionHandler(final String op) {
    return (lhs, rhs) -> {
      final Object parsedLhs = lhs.accept(lhsParser);
      identifiers.addAll(lhsParser.getIdentifiers());

      final Object parsedRhs = rhs.accept(rhsParser);
      identifiers.addAll(rhsParser.getIdentifiers());

      return Map.of(EXPR, new BasicDBObject(PREFIX + op, new Object[] {parsedLhs, parsedRhs}));
    };
  }

  private static BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>>
      unknownHandler(final RelationalOperator operator) {
    return (lhs, rhs) -> {
      throw getUnsupportedOperationException(operator);
    };
  }
}
