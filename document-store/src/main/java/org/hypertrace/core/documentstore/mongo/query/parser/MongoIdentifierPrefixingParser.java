package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

final class MongoIdentifierPrefixingParser extends MongoSelectTypeExpressionParser {

  MongoIdentifierPrefixingParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return Optional.ofNullable(baseParser.visit(expression)).map(id -> PREFIX + id).orElse(null);
  }
}
