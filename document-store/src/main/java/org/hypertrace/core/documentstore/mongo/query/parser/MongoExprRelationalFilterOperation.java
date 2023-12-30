package org.hypertrace.core.documentstore.mongo.query.parser;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
class MongoExprRelationalFilterOperation implements RelationalFilterOperation {
  public static final String EXPR = "$expr";

  private final MongoSelectTypeExpressionParser lhsParser;
  private final MongoSelectTypeExpressionParser rhsParser;
  private final String operator;

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    final Object parsedLhs = lhs.accept(lhsParser);
    final Object parsedRhs = rhs.accept(rhsParser);
    return Map.of(operator, new Object[] {parsedLhs, parsedRhs});
  }
}
