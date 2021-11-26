package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Query;

final class MongoIdentifierExpressionParser extends MongoSelectingExpressionParser {

  MongoIdentifierExpressionParser(final Query query) {
    super(query);
  }

  MongoIdentifierExpressionParser(final MongoSelectingExpressionParser baseParser) {
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
