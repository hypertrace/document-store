package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;
import org.hypertrace.core.documentstore.query.Query;

public class MongoAggregateExpressionParser extends MongoExpressionParser {

  protected MongoAggregateExpressionParser(final Query query) {
    super(query);
  }

  Map<String, Object> parse(final AggregateExpression expression) {
    String key;

    if (expression.getAggregator() == AggregationOperator.DISTINCT_COUNT) {
      key = "$addToSet";
    } else {
      key = "$" + expression.getAggregator().name().toLowerCase();
    }

    SelectingExpressionParser parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoSelectingExpressionParser(query));
    Object value = expression.getExpression().parse(parser);
    return Map.of(key, value);
  }
}
