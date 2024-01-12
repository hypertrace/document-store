package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

final class MongoIdentifierPrefixingParser extends MongoSelectTypeExpressionParser {
  private final String prefix;

  MongoIdentifierPrefixingParser(final MongoSelectTypeExpressionParser baseParser) {
    this(baseParser, PREFIX);
  }

  MongoIdentifierPrefixingParser(
      final MongoSelectTypeExpressionParser baseParser, final String prefix) {
    super(baseParser);
    this.prefix = prefix;
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return Optional.ofNullable(baseParser.visit(expression)).map(id -> prefix + id).orElse(null);
  }
}
