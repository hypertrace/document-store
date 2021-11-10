package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;

public class MongoSelectingExpressionParser implements SelectingExpressionParser {

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
