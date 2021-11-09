package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.AggregateExpression;
import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;
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
