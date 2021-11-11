package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;

public class MongoConstantExpressionParser {

  static Object parse(final ConstantExpression expression) {
    return expression.getValue();
  }
}
