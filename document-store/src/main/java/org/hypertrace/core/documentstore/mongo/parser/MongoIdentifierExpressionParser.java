package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.IdentifierExpression;

public class MongoIdentifierExpressionParser {
  static String parse(IdentifierExpression expression) {
    return expression.getName();
  }
}
