package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Query;

final class MongoIdentifierExpressionParser extends MongoExpressionParser {

  MongoIdentifierExpressionParser(Query query) {
    super(query);
  }

  String parse(final IdentifierExpression expression) {
    return expression.getName();
  }
}
