package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Query;

public class MongoIdentifierExpressionParser extends MongoExpressionParser {

  protected MongoIdentifierExpressionParser(Query query) {
    super(query);
  }

  String parse(final IdentifierExpression expression) {
    return expression.getName();
  }
}
