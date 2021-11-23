package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

final class MongoIdentifierPrefixingSelectingExpressionParser
    extends MongoSelectingExpressionParser {
  private final MongoSelectingExpressionParser baseParser;

  MongoIdentifierPrefixingSelectingExpressionParser(
      final MongoSelectingExpressionParser baseParser) {
    super(baseParser.query);
    this.baseParser = baseParser;
  }

  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    return baseParser.visit(expression);
  }

  @Override
  public Object visit(final ConstantExpression expression) {
    return baseParser.visit(expression);
  }

  @Override
  public Map<String, Object> visit(final FunctionExpression expression) {
    return baseParser.visit(expression);
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    return Optional.ofNullable(baseParser.visit(expression)).map(id -> "$" + id).orElse(null);
  }
}
