package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public class MongoAggregateExpressionParser extends MongoExpressionParser {

  protected MongoAggregateExpressionParser(final Query query) {
    super(query);
  }

  Map<String, Object> parse(final AggregateExpression expression) {
    String key;

    if (expression.getAggregator() == AggregationOperator.DISTINCT_COUNT) {
      // Since there is no direct support for DISTINCT_COUNT (along with aggregations) in MongoDB,
      // we split it into 2 steps:
      //   (a) Add to set (so that we get the distinct values)
      //   (b) Get the size of the set (so that we get the count of distinct values)
      // Note: (b) would already have been added by MongoQueryTransformer
      key = "$addToSet";
    } else {
      key = "$" + expression.getAggregator().name().toLowerCase();
    }

    SelectingExpressionVisitor parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoSelectingExpressionParser(query));
    Object value = expression.getExpression().visit(parser);
    return Map.of(key, value);
  }
}
