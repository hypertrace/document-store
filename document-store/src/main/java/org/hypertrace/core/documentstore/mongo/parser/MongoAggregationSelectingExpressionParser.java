package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public class MongoAggregationSelectingExpressionParser extends MongoSelectingExpressionParser {
  private final SelectionSpec source;

  public MongoAggregationSelectingExpressionParser(final Query query, final SelectionSpec source) {
    super(query);
    this.source = source;
  }

  @Override
  public Map<String, Object> parse(AggregateExpression expression) {
    return new MongoAggregateExpressionParser(query, source).parse(expression);
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
