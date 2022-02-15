package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

final class MongoIdentifierPrefixingExpressionParser extends MongoSelectableExpressionParser {

  MongoIdentifierPrefixingExpressionParser(final MongoSelectableExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return Optional.ofNullable(baseParser.visit(expression)).map(id -> PREFIX + id).orElse(null);
  }
}
