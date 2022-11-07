package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableSet;

import java.util.HashSet;
import java.util.Set;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.SubQueryIdentifierExpression;

final class MongoIdentifierCollectingParser extends MongoSelectTypeExpressionParser {
  private final Set<String> identifiers = new HashSet<>();

  MongoIdentifierCollectingParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    identifiers.add(expression.getName());
    return baseParser.visit(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final SubQueryIdentifierExpression expression) {
    return baseParser.visit(expression);
  }

  public Set<String> getIdentifiers() {
    return unmodifiableSet(identifiers);
  }
}
