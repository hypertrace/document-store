package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
class MongoRelationalFilterOperation implements RelationalFilterOperation {
  private final MongoSelectTypeExpressionParser lhsParser;
  private final MongoSelectTypeExpressionParser rhsParser;
  private final String operator;

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    final String parsedLhs = lhs.accept(lhsParser);
    final Object parsedRhs = rhs.accept(rhsParser);
    return Map.of(parsedLhs, new BasicDBObject(operator, parsedRhs));
  }
}
