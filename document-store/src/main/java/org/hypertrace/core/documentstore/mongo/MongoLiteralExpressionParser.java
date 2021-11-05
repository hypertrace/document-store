package org.hypertrace.core.documentstore.mongo;

import org.hypertrace.core.documentstore.expression.LiteralExpression;

public class MongoLiteralExpressionParser extends MongoExpressionParser<LiteralExpression> {

  public MongoLiteralExpressionParser(LiteralExpression expression) {
    super(expression);
  }

  @Override
  public String parse() {
    return "$" + expression.getLiteral();
  }
}
