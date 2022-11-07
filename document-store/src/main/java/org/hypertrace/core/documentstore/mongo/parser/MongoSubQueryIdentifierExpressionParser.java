package org.hypertrace.core.documentstore.mongo.parser;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.SubQueryIdentifierExpression;

@NoArgsConstructor
final class MongoSubQueryIdentifierExpressionParser extends MongoSelectTypeExpressionParser {

  MongoSubQueryIdentifierExpressionParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final SubQueryIdentifierExpression expression) {
    return parse(expression);
  }

  String parse(final SubQueryIdentifierExpression expression) {
    return expression.getName();
  }
}
