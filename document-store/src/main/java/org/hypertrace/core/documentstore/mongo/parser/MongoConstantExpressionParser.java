package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.ConstantExpression;

public class MongoConstantExpressionParser {
  static Object parse(ConstantExpression expression) {
    return expression.getValue();
  }
}
