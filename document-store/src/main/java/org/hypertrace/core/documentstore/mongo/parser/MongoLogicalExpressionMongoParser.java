package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.LogicOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicOperator.OR;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.LogicExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicOperator;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

final class MongoLogicalExpressionMongoParser {
  private static final Map<LogicOperator, String> KEY_MAP =
      unmodifiableMap(
          new EnumMap<>(LogicOperator.class) {
            {
              put(AND, "$and");
              put(OR, "$or");
            }
          });

  Map<String, Object> parse(final LogicExpression expression) {
    LogicOperator operator = expression.getOperator();
    String key = KEY_MAP.get(operator);

    if (key == null) {
      throw getUnsupportedOperationException(operator);
    }

    FilterTypeExpressionVisitor parser = new MongoFilterTypeExpressionParser();
    List<Object> parsed =
        expression.getOperands().stream()
            .map(exp -> exp.accept(parser))
            .collect(Collectors.toList());

    return Map.of(key, parsed);
  }
}
