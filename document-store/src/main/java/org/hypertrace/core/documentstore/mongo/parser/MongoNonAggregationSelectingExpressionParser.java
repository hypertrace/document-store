package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.query.Query;

final class MongoNonAggregationSelectingExpressionParser extends MongoSelectingExpressionParser {

  MongoNonAggregationSelectingExpressionParser(final Query query) {
    super(query);
  }

  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    return null;
  }
}
