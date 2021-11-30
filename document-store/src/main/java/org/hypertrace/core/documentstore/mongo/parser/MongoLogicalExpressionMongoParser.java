package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.parser.FilteringExpressionVisitor;

final class MongoLogicalExpressionMongoParser {
  private static final Map<LogicalOperator, String> KEY_MAP =
      unmodifiableMap(
          new EnumMap<>(LogicalOperator.class) {
            {
              put(AND, "$and");
              put(OR, "$or");
            }
          });

  Map<String, Object> parse(final LogicalExpression expression) {
    LogicalOperator operator = expression.getOperator();
    String key = KEY_MAP.get(operator);

    if (key == null) {
      throw getUnsupportedOperationException(operator);
    }

    FilteringExpressionVisitor parser = new MongoFilteringExpressionParser();
    List<Object> parsed =
        expression.getOperands().stream()
            .map(exp -> exp.visit(parser))
            .collect(Collectors.toList());

    return Map.of(key, parsed);
  }
}
