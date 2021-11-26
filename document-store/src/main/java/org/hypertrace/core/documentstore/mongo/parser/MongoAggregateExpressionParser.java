package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.mongo.parser.MongoParserUtils.getUnsupportedOperationException;

import java.util.EnumMap;
import java.util.Map;
import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;

@NoArgsConstructor
final class MongoAggregateExpressionParser extends MongoSelectingExpressionParser {
  private static final Map<AggregationOperator, String> KEY_MAP =
      unmodifiableMap(
          new EnumMap<>(AggregationOperator.class) {
            {
              put(AVG, "$avg");
              put(DISTINCT, "$addToSet");
              put(SUM, "$sum");
              put(MIN, "$min");
              put(MAX, "$max");
            }
          });

  MongoAggregateExpressionParser(final MongoSelectingExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    return parse(expression);
  }

  Map<String, Object> parse(final AggregateExpression expression) {
    AggregationOperator operator = expression.getAggregator();
    String key = KEY_MAP.get(operator);

    if (key == null) {
      throw getUnsupportedOperationException(operator);
    }

    SelectingExpressionVisitor parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoIdentifierExpressionParser(
                new MongoAggregateExpressionParser(
                    new MongoFunctionExpressionParser(new MongoConstantExpressionParser()))));

    Object value = expression.getExpression().visit(parser);
    return Map.of(key, value);
  }
}
