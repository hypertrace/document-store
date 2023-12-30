package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
class MongoLikeFilterOperation implements RelationalFilterOperation {
  private static final String IGNORE_CASE_OPTION = "i";
  private static final String OPTIONS = "$options";
  private static final String REGEX = "$regex";

  private final MongoSelectTypeExpressionParser lhsParser;
  private final MongoSelectTypeExpressionParser rhsParser;

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    final String parsedLhs = lhs.accept(lhsParser);
    final Object parsedRhs = rhs.accept(rhsParser);
    return Map.of(
        parsedLhs, new BasicDBObject(REGEX, parsedRhs).append(OPTIONS, IGNORE_CASE_OPTION));
  }
}
