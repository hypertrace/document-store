package org.hypertrace.core.documentstore.mongo.query.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_ARRAY;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.LAST;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

@NoArgsConstructor
final class MongoAggregateExpressionParser extends MongoSelectTypeExpressionParser {
  private static final Map<AggregationOperator, String> KEY_MAP =
      unmodifiableMap(
          new EnumMap<>(AggregationOperator.class) {
            {
              put(AVG, "$avg");
              put(DISTINCT, "$addToSet");
              put(DISTINCT_ARRAY, "$addToSet");
              put(SUM, "$sum");
              put(MIN, "$min");
              put(MAX, "$max");
              put(LAST, "$last");
            }
          });

  MongoAggregateExpressionParser(final MongoSelectTypeExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    return parse(expression);
  }

  Map<String, Object> parse(final AggregateExpression expression) {
    AggregationOperator operator = expression.getAggregator();

    SelectTypeExpressionVisitor parser =
        new MongoIdentifierPrefixingParser(
            new MongoIdentifierExpressionParser(
                new MongoAggregateExpressionParser(
                    new MongoFunctionExpressionParser(new MongoConstantExpressionParser()))));

    // MongoDB has no native COUNT accumulator. Implement COUNT with $sum instead of collecting
    // every value into an array via $push (followed by $size). The $push approach materializes one
    // array element per matching document, which is memory-intensive and can spill to disk.
    //
    // The previous $push semantics are preserved:
    //  - COUNT(<constant>) counts every document in the group (i.e. COUNT(*)).
    //  - COUNT(<field/expr>) counts only documents where the operand is present (not missing),
    //    matching $push, which skips missing values. ($type returns "missing" for absent fields.)
    if (operator == COUNT) {
      if (expression.getExpression() instanceof ConstantExpression) {
        return Map.of("$sum", 1);
      }

      Object operand = expression.getExpression().accept(parser);
      return Map.of(
          "$sum",
          Map.of("$cond", List.of(Map.of("$ne", List.of(Map.of("$type", operand), "missing")), 1, 0)));
    }

    String key = KEY_MAP.get(operator);

    if (key == null) {
      throw getUnsupportedOperationException(operator);
    }

    Object value = expression.getExpression().accept(parser);
    return Map.of(key, value);
  }
}
