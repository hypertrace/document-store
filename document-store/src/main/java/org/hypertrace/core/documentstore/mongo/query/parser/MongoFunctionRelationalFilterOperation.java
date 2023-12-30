package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
class MongoFunctionRelationalFilterOperation implements RelationalFilterOperation {
  private static final String EXPR = "$expr";

  private final MongoSelectTypeExpressionParser functionParser =
      new MongoFunctionExpressionParser();

  private final MongoSelectTypeExpressionParser lhsParser;
  private final MongoSelectTypeExpressionParser rhsParser;
  private final String operator;

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    try {
      final Object parsedLhs = lhs.accept(functionParser);
      final Object parsedRhs = rhs.accept(rhsParser);
      return Map.of(EXPR, new BasicDBObject(operator, new Object[] {parsedLhs, parsedRhs}));
    } catch (final UnsupportedOperationException e) {
      // Fallback if the LHS was not a function
      return new MongoRelationalFilterOperation(lhsParser, rhsParser, operator).apply(lhs, rhs);
    }
  }
}
