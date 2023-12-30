package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
class MongoStartsWithFilterOperation implements RelationalFilterOperation {
  private static final String REGEX = "$regex";
  private static final String STARTS_WITH_PREFIX = "^";

  private final MongoSelectTypeExpressionParser lhsParser;
  private final MongoSelectTypeExpressionParser rhsParser;

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    final String parsedLhs = lhs.accept(lhsParser);
    final Object parsedRhs = rhs.accept(rhsParser);
    // Since starts with makes sense only for string values, the RHS is converted to a string
    final String rhsValue = STARTS_WITH_PREFIX + parsedRhs;
    return Map.of(parsedLhs, new BasicDBObject(REGEX, rhsValue));
  }
}
