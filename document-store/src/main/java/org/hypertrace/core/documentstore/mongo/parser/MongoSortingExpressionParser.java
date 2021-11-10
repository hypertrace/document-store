package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SortingExpressionParser;

public class MongoSortingExpressionParser implements SortingExpressionParser {

  @Override
  public Object parse(AggregateExpression expression) {
    return null;
  }

  @Override
  public Object parse(FunctionExpression expression) {
    return null;
  }

  @Override
  public Object parse(IdentifierExpression expression) {
    return null;
  }
}
