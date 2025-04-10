package org.hypertrace.core.documentstore.mongo.query.parser;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.mongo.MongoUtils;

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

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final AliasedIdentifierExpression expression) {
    return MongoUtils.PREFIX + MongoUtils.encodeVariableName(parse(expression));
  }

  String parse(final IdentifierExpression expression) {
    return expression.getName();
  }

  String parse(final AliasedIdentifierExpression expression) {
    return expression.getName();
  }
}
