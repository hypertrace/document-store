package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.PREFIX;

import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
public class MongoFunctionRelationalFilterOperation
    implements BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>> {
  private static final String EXPR = "$expr";

  private static final MongoSelectTypeExpressionParser lhsParser =
      new MongoFunctionExpressionParser();

  // Only a constant RHS is supported as of now
  private static final MongoSelectTypeExpressionParser rhsParser =
      new MongoConstantExpressionParser();
  private final String operator;

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    try {
      final Object parsedLhs = lhs.accept(lhsParser);
      final Object parsedRhs = rhs.accept(rhsParser);
      return Map.of(
          EXPR, new BasicDBObject(PREFIX + operator, new Object[] {parsedLhs, parsedRhs}));
    } catch (final UnsupportedOperationException e) {
      // Fallback if the LHS was not a function
      return new MongoRelationalFilterOperation(operator).apply(lhs, rhs);
    }
  }
}
