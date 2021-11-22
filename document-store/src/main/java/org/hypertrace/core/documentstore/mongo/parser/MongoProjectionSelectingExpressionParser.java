package org.hypertrace.core.documentstore.mongo.parser;

import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public class MongoProjectionSelectingExpressionParser extends MongoSelectingExpressionParser {
  private static final String UNKNOWN_FIELD_PREFIX = "unknown_field_";

  private final MongoSelectingExpressionParser baseParser;
  private final String alias;

  public MongoProjectionSelectingExpressionParser(
      final String alias, final MongoSelectingExpressionParser baseParser) {
    super(baseParser.query);
    this.alias = alias;
    this.baseParser = baseParser;
  }

  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    return convertToMap(baseParser.visit(expression));
  }

  @Override
  public Map<String, Object> visit(final ConstantExpression expression) {
    return convertToMap(baseParser.visit(expression));
  }

  @Override
  public Map<String, Object> visit(final FunctionExpression expression) {
    return convertToMap(baseParser.visit(expression));
  }

  @Override
  public Map<String, Object> visit(final IdentifierExpression expression) {
    Object parsed = baseParser.visit(expression);
    if (parsed == null) {
      return Map.of();
    }

    if (StringUtils.isBlank(alias)) {
      String key = StringUtils.stripStart(parsed.toString(), "$");
      return Map.of(key, 1);
    }

    return Map.of(alias, parsed);
  }

  private String getAlias() {
    if (StringUtils.isNotBlank(alias)) {
      return alias;
    }

    return UNKNOWN_FIELD_PREFIX + UUID.randomUUID().toString().replaceAll("-", "_");
  }

  private Map<String, Object> convertToMap(final Object parsed) {
    if (parsed == null) {
      return Map.of();
    }

    return Map.of(getAlias(), parsed);
  }
}
