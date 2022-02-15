package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.encodeKey;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@Slf4j
public final class MongoProjectingParser extends MongoSelectTypeExpressionParser {
  private final String alias;

  MongoProjectingParser(final String alias, final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
    this.alias = alias;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    final Object parsed;

    try {
      parsed = baseParser.visit(expression);
    } catch (UnsupportedOperationException e) {
      return Map.of();
    }

    final String key = getEncodedAlias(expression);
    return convertToMap(key, parsed);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final ConstantExpression expression) {
    final Object parsed;

    try {
      parsed = baseParser.visit(expression);
    } catch (UnsupportedOperationException e) {
      return Map.of();
    }

    final String key = getAlias(expression);
    return convertToMap(key, parsed);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final FunctionExpression expression) {
    final Object parsed;

    try {
      parsed = baseParser.visit(expression);
    } catch (UnsupportedOperationException e) {
      return Map.of();
    }

    final String key = getEncodedAlias(expression);
    return convertToMap(key, parsed);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final IdentifierExpression expression) {
    final String parsed;

    try {
      parsed = baseParser.visit(expression);
    } catch (UnsupportedOperationException e) {
      return Map.of();
    }

    final String key;

    if (StringUtils.isBlank(alias)) {
      key = StringUtils.stripStart(parsed, PREFIX);
      return convertToMap(key, 1);
    }

    key = getAlias(expression);
    return convertToMap(key, parsed);
  }

  private Map<String, Object> convertToMap(final String key, final Object parsed) {
    return parsed == null ? Map.of() : Map.of(key, parsed);
  }

  private String getEncodedAlias(final SelectTypeExpression expression) {
    return encodeKey(getAlias(expression));
  }

  private String getAlias(final SelectTypeExpression expression) {
    if (StringUtils.isBlank(alias)) {
      throw new IllegalArgumentException(
          String.format("Alias is must for: %s", expression.toString()));
    }

    return alias;
  }
}
