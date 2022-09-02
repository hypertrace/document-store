package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.encodeKey;

import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public class MongoRelationalLhsExpressionParser extends MongoSelectTypeExpressionParser {
  private final MongoIdentifierExpressionParser mongoIdentifierExpressionParser =
      new MongoIdentifierExpressionParser();

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final FunctionExpression expression) {
    return encodeKey(expression.toString());
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return expression.accept(mongoIdentifierExpressionParser);
  }
}
