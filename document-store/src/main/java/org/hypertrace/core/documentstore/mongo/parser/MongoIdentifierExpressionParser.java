package org.hypertrace.core.documentstore.mongo.parser;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

@NoArgsConstructor
final class MongoIdentifierExpressionParser extends MongoSelectTypeExpressionParser {

  MongoIdentifierExpressionParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return parse(expression);
  }

  String parse(final IdentifierExpression expression) {
    return expression.getName();
  }
}
