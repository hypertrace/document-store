package org.hypertrace.core.documentstore.mongo.query.parser;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

@NoArgsConstructor
public final class MongoIdentifierExpressionParser extends MongoSelectTypeExpressionParser {

  MongoIdentifierExpressionParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return parse(expression);
  }

  @Override
  public String visit(final AliasedIdentifierExpression expression) {
    return "$" + parse(expression);
  }

  String parse(final IdentifierExpression expression) {
    return expression.getName();
  }
}
