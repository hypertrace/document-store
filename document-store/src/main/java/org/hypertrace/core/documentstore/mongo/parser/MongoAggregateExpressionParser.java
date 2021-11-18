package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;
import org.hypertrace.core.documentstore.query.Query;

public class MongoAggregateExpressionParser extends MongoExpressionParser {

  protected MongoAggregateExpressionParser(Query query) {
    super(query);
  }

  Map<String, Object> parse(final AggregateExpression expression) {
    String key = "$" + expression.getAggregator().name().toLowerCase();

    SelectingExpressionParser parser = new MongoSelectingExpressionParser(query, true);
    SelectingExpression innerExpression = expression.getExpression();

    Object value = innerExpression.parse(parser);

    return Map.of(key, value);
  }
}
