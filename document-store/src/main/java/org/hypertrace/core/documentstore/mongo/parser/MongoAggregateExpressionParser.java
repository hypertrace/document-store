package org.hypertrace.core.documentstore.mongo.parser;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.Function;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public class MongoAggregateExpressionParser extends MongoExpressionParser {
  private static final Map<AggregationOperator, Function<AggregationOperator, String>>
      KEY_PROVIDER_MAP =
          ImmutableMap.<AggregationOperator, Function<AggregationOperator, String>>builder()
              .put(DISTINCT_COUNT, getKeyForDistinctCount())
              .put(DISTINCT, op -> "addToSet")
              .build();

  protected MongoAggregateExpressionParser(final Query query) {
    super(query);
  }

  Map<String, Object> parse(final AggregateExpression expression) {
    AggregationOperator operator = expression.getAggregator();
    Function<AggregationOperator, String> keyProvider =
        KEY_PROVIDER_MAP.getOrDefault(operator, getDefaultKey());
    String key = "$" + keyProvider.apply(operator);

    SelectingExpressionVisitor parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoSelectingExpressionParser(query));
    Object value = expression.getExpression().visit(parser);
    return Map.of(key, value);
  }

  private static Function<AggregationOperator, String> getKeyForDistinctCount() {
    return op -> {
      throw new UnsupportedOperationException(
          "$distinctCount (aggregation) is not supported in MongoDB");
    };
  }

  private static Function<AggregationOperator, String> getDefaultKey() {
    return op -> op.name().toLowerCase();
  }
}
