package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.IGroupingExpressionParser;

public class MongoGroupingExpressionParser implements IGroupingExpressionParser {

  @Override
  public Object parse(FunctionExpression expression) {
    return null;
  }

  @Override
  public Object parse(IdentifierExpression expression) {
    return null;
  }
}
