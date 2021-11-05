package org.hypertrace.core.documentstore.mongo;

import org.hypertrace.core.documentstore.expression.LiteralExpression;

public class MongoLiteralExpressionParser implements MongoExpressionParser<LiteralExpression> {
  @Override
  public String parseExpression(LiteralExpression expression) {
    return "$" + expression.getLiteral();
  }
}
