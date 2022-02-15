package org.hypertrace.core.documentstore.mongo.parser;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;

@NoArgsConstructor
final class MongoConstantExpressionParser extends MongoSelectTypeExpressionParser {
  MongoConstantExpressionParser(final MongoSelectTypeExpressionParser baseParser) {
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
