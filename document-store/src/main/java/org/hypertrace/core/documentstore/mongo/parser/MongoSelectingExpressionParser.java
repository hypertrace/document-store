package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.AggregateExpression;
import org.hypertrace.core.documentstore.expression.ConstantExpression;
import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.ISelectingExpressionParser;

public class MongoSelectingExpressionParser implements ISelectingExpressionParser {

  @Override
  public Object parse(AggregateExpression expression) {
    return null;
  }

  @Override
  public Object parse(ConstantExpression expression) {
    return MongoConstantExpressionParser.parse(expression);
  }

  @Override
  public Object parse(FunctionExpression expression) {
    return null;
  }

  @Override
  public Object parse(IdentifierExpression expression) {
    return MongoIdentifierExpressionParser.parse(expression);
  }
}
