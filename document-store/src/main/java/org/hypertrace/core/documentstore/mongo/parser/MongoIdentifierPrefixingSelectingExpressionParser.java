package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public class MongoIdentifierPrefixingSelectingExpressionParser
    extends MongoSelectingExpressionParser {
  private final MongoSelectingExpressionParser baseParser;

  public MongoIdentifierPrefixingSelectingExpressionParser(
      final MongoSelectingExpressionParser baseParser) {
    super(baseParser.query);
    this.baseParser = baseParser;
  }

  @Override
  public Map<String, Object> parse(final AggregateExpression expression) {
    return baseParser.parse(expression);
  }

  @Override
  public Object parse(final ConstantExpression expression) {
    return baseParser.parse(expression);
  }

  @Override
  public Map<String, Object> parse(final FunctionExpression expression) {
    return baseParser.parse(expression);
  }

  @Override
  public String parse(final IdentifierExpression expression) {
    return Optional.ofNullable(baseParser.parse(expression)).map(id -> "$" + id).orElse(null);
  }
}
