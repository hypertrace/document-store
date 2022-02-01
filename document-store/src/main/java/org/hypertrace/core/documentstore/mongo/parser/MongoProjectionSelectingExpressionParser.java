package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.mongo.MongoUtils;

@Slf4j
public final class MongoProjectionSelectingExpressionParser extends MongoSelectingExpressionParser {
  private final String alias;

  MongoProjectionSelectingExpressionParser(
      final String alias, final MongoSelectingExpressionParser baseParser) {
    super(baseParser);
    this.alias = alias;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    try {
      return convertToMap(baseParser.visit(expression), expression);
    } catch (UnsupportedOperationException e) {
      return Map.of();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final ConstantExpression expression) {
    try {
      return convertToMap(baseParser.visit(expression), expression);
    } catch (UnsupportedOperationException e) {
      return Map.of();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final FunctionExpression expression) {
    try {
      return convertToMap(baseParser.visit(expression), expression);
    } catch (UnsupportedOperationException e) {
      return Map.of();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final IdentifierExpression expression) {
    Object parsed;

    try {
      parsed = baseParser.visit(expression);
    } catch (UnsupportedOperationException e) {
      return Map.of();
    }

    if (StringUtils.isBlank(alias)) {
      String key = StringUtils.stripStart(parsed.toString(), PREFIX);
      key = MongoUtils.encodeKey(key);
      return Map.of(key, 1);
    }

    return Map.of(alias, parsed);
  }

  private Map<String, Object> convertToMap(
      final Object parsed, final SelectingExpression expression) {
    if (parsed == null) {
      return Map.of();
    }

    String key = getAlias(expression);
    return Map.of(key, parsed);
  }

  private String getAlias(final SelectingExpression expression) {
    if (StringUtils.isBlank(alias)) {
      throw new IllegalArgumentException(
          String.format("Alias is must for: %s", expression.toString()));
    }

    return MongoUtils.encodeKey(alias);
  }
}
