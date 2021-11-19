package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Query;

public class MongoAggregationSelectingExpressionParser extends MongoSelectingExpressionParser {

  public MongoAggregationSelectingExpressionParser(final Query query) {
    super(query);
  }

  @Override
  public Map<String, Object> parse(AggregateExpression expression) {
    return new MongoAggregateExpressionParser(query).parse(expression);
  }

  @Override
  public Object parse(final ConstantExpression expression) {
    return null;
  }

  @Override
  public Map<String, Object> parse(final FunctionExpression expression) {
    return null;
  }

  @Override
  public Object parse(final IdentifierExpression expression) {
    return null;
  }
}
