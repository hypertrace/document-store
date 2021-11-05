package org.hypertrace.core.documentstore.mongo;

import org.hypertrace.core.documentstore.expression.ConstantExpression;

public class MongoConstantExpressionParser extends MongoExpressionParser<ConstantExpression> {

  public MongoConstantExpressionParser(ConstantExpression expression) {
    super(expression);
  }

  @Override
  public Object parse() {
    return expression.getConstant();
  }
}
