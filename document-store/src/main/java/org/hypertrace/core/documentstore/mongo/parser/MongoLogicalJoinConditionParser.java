package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.mongo.parser.MongoJoinConditionParser.MongoJoinParseResult;

final class MongoLogicalJoinConditionParser {
  private static final Map<LogicalOperator, String> KEY_MAP =
      unmodifiableMap(
          new EnumMap<>(LogicalOperator.class) {
            {
              put(AND, "$and");
              put(OR, "$or");
            }
          });

  MongoJoinParseResult parse(final LogicalExpression expression) {
    LogicalOperator operator = expression.getOperator();
    String key = KEY_MAP.get(operator);

    if (key == null) {
      throw getUnsupportedOperationException(operator);
    }

    final MongoJoinConditionParser parser = new MongoJoinConditionParser();
    final List<MongoJoinParseResult> parsed =
        expression.getOperands().stream()
            .map(exp -> exp.accept(parser))
            .map(MongoJoinParseResult.class::cast)
            .collect(toUnmodifiableList());
    final List<Object> parsedFilters =
        parsed.stream().map(MongoJoinParseResult::getParsedFilter).collect(toUnmodifiableList());
    final Set<String> variables =
        parsed.stream()
            .map(MongoJoinParseResult::getVariables)
            .flatMap(Set::stream)
            .collect(toUnmodifiableSet());
    final Map<String, Object> parsedFilter = Map.of(key, parsedFilters);

    return MongoJoinParseResult.builder().parsedFilter(parsedFilter).variables(variables).build();
  }
}
