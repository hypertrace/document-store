package org.hypertrace.core.documentstore.mongo.parser;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.query.Query;

final class MongoConstantExpressionParser extends MongoSelectingExpressionParser {

  MongoConstantExpressionParser(final Query query) {
    super(query);
  }

  MongoConstantExpressionParser(final MongoSelectingExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object visit(final ConstantExpression expression) {
    return parse(expression);
  }

  Object parse(final ConstantExpression expression) {
    return expression.getValue();
  }
}
