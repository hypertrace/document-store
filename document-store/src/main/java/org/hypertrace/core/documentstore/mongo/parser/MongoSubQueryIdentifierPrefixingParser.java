package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.SubQueryIdentifierExpression;

final class MongoSubQueryIdentifierPrefixingParser extends MongoSelectTypeExpressionParser {

  MongoSubQueryIdentifierPrefixingParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return Optional.ofNullable(baseParser.visit(expression)).map(Object::toString).orElse(null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final SubQueryIdentifierExpression expression) {
    return Optional.ofNullable(baseParser.visit(expression)).map(id -> PREFIX + id).orElse(null);
  }
}
