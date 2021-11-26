package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

final class MongoIdentifierPrefixingSelectingExpressionParser
    extends MongoSelectingExpressionParser {

  MongoIdentifierPrefixingSelectingExpressionParser(final MongoSelectingExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    assert baseParser != null;
    return Optional.ofNullable(baseParser.visit(expression)).map(id -> "$" + id).orElse(null);
  }
}
