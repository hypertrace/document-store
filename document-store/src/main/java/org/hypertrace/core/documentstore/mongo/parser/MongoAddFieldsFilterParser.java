package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.encodeKey;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

public class MongoAddFieldsFilterParser implements FilterTypeExpressionVisitor {

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final LogicalExpression expression) {
    return expression.getOperands().stream()
        .map(this::parse)
        .map(Map::entrySet)
        .flatMap(Set::stream)
        .collect(toUnmodifiableMap(Entry::getKey, Entry::getValue));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final RelationalExpression expression) {
    final MongoSelectTypeExpressionParser lhsParser = new MongoFunctionExpressionParser();
    final SelectTypeExpression lhs = expression.getLhs();
    final Map<String, Object> parsedLhs;

    try {
      parsedLhs = lhs.accept(lhsParser);
    } catch (final UnsupportedOperationException e) {
      return emptyMap();
    }

    final String key = encodeKey(lhs.toString());
    return Map.of(key, parsedLhs);
  }

  private Map<String, Object> parse(final FilterTypeExpression operand) {
    return operand.accept(this);
  }
}
