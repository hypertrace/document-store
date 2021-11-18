package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.query.Query;

public class MongoConstantExpressionParser extends MongoExpressionParser {

  protected MongoConstantExpressionParser(Query query) {
    super(query);
  }

  Object parse(final ConstantExpression expression) {
    return expression.getValue();
  }
}
