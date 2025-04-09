package org.hypertrace.core.documentstore.mongo.query.parser;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;

@NoArgsConstructor
public final class MongoAliasedIdentifierExpressionParser extends MongoSelectTypeExpressionParser {

  MongoAliasedIdentifierExpressionParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final AliasedIdentifierExpression expression) {
    return "$" + parse(expression);
  }

  String parse(final AliasedIdentifierExpression expression) {
    return expression.getName();
  }
}
