package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

final class MongoDollarPrefixingIdempotentParser extends MongoSelectTypeExpressionParser {
  MongoDollarPrefixingIdempotentParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return Optional.ofNullable(baseParser.visit(expression))
        .map(Object::toString)
        .map(this::idempotentPrefix)
        .orElse(null);
  }

  private String idempotentPrefix(final String identifier) {
    return identifier.startsWith(PREFIX) ? identifier : PREFIX + identifier;
  }
}
