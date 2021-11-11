package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;

public class MongoAggregateExpressionParser {

  static Map<String, Object> parse(final AggregateExpression expression) {
    String key = "$" + expression.getAggregator().name().toLowerCase();

    SelectingExpressionParser parser = new MongoSelectingExpressionParser();
    Object value = expression.getExpression().parse(parser);

    return Map.of(key, value);
  }
}
