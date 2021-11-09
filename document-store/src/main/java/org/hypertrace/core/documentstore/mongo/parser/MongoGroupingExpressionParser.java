package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.GroupingExpressionParser;

public class MongoGroupingExpressionParser implements GroupingExpressionParser {

  @Override
  public Object parse(FunctionExpression expression) {
    return null;
  }

  @Override
  public Object parse(IdentifierExpression expression) {
    return null;
  }
}
