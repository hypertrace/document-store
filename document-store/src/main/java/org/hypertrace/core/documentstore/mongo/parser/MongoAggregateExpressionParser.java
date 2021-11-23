package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.FIRST;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;

import java.util.EnumMap;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

final class MongoAggregateExpressionParser extends MongoExpressionParser {
  private static final Map<AggregationOperator, String> KEY_MAP =
      unmodifiableMap(
          new EnumMap<>(AggregationOperator.class) {
            {
              put(AVG, "$avg");
              put(DISTINCT, "$addToSet");
              put(SUM, "$sum");
              put(MIN, "$min");
              put(MAX, "$max");
              put(FIRST, "$first");
            }
          });

  MongoAggregateExpressionParser(final Query query) {
    super(query);
  }

  Map<String, Object> parse(final AggregateExpression expression) {
    AggregationOperator operator = expression.getAggregator();
    String key = KEY_MAP.get(operator);

    if (key == null) {
      throw getUnsupportedOperationException(operator);
    }

    SelectingExpressionVisitor parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoSelectingExpressionParser(query));
    Object value = expression.getExpression().visit(parser);
    return Map.of(key, value);
  }
}
