package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
class MongoNotContainsFilterOperation implements RelationalFilterOperation {
  private final MongoSelectTypeExpressionParser lhsParser;
  private final MongoSelectTypeExpressionParser rhsParser;

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    final String parsedLhs = lhs.accept(lhsParser);
    final Object parsedRhs = rhs.accept(rhsParser);
    return Map.of(parsedLhs, new BasicDBObject("$not", buildElemMatch(parsedRhs)));
  }

  private static BasicDBObject buildElemMatch(final Object parsedRhs) {
    return new BasicDBObject("$elemMatch", new BasicDBObject("$eq", parsedRhs));
  }
}
