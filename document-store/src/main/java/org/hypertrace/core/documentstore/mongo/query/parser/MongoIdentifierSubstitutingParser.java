package org.hypertrace.core.documentstore.mongo.query.parser;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

final class MongoIdentifierSubstitutingParser extends MongoSelectTypeExpressionParser {
  private final String sourceToMatch;
  private final String targetToSubstitute;

  MongoIdentifierSubstitutingParser(
      final MongoSelectTypeExpressionParser baseParser,
      final String sourceToMatch,
      final String targetToSubstitute) {
    super(baseParser);
    this.sourceToMatch = sourceToMatch;
    this.targetToSubstitute = targetToSubstitute;
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final IdentifierExpression expression) {
    return Optional.ofNullable(baseParser.visit(expression))
        .map(Object::toString)
        .map(this::substituteIfApplicable)
        .orElse(null);
  }

  private String substituteIfApplicable(final String identifier) {
    return sourceToMatch.equals(identifier) ? targetToSubstitute : identifier;
  }
}
