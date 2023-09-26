package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
public class MongoNotContainsFilterOperation
    implements BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>> {
  private static final MongoSelectTypeExpressionParser identifierParser =
      new MongoIdentifierExpressionParser();
  // Only a constant RHS is supported as of now
  private static final MongoSelectTypeExpressionParser rhsParser =
      new MongoConstantExpressionParser();

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    final String parsedLhs = lhs.accept(identifierParser);
    final Object parsedRhs = rhs.accept(rhsParser);
    return Map.of(parsedLhs, new BasicDBObject("$not", buildElemMatch(parsedRhs)));
  }

  private static BasicDBObject buildElemMatch(final Object parsedRhs) {
    return new BasicDBObject("$elemMatch", new BasicDBObject("$eq", parsedRhs));
  }
}
