package org.hypertrace.core.documentstore.mongo.query.parser;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.mongo.MongoUtils;

@NoArgsConstructor
public final class MongoAliasedIdentifierExpressionParser extends MongoSelectTypeExpressionParser {

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final AliasedIdentifierExpression expression) {
    return MongoUtils.PREFIX + parse(expression);
  }

  String parse(final AliasedIdentifierExpression expression) {
    return expression.getName();
  }
}
