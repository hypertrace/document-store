package org.hypertrace.core.documentstore.mongo.query.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

@AllArgsConstructor
public class MongoLikeFilterOperation
    implements BiFunction<SelectTypeExpression, SelectTypeExpression, Map<String, Object>> {
  private static final String IGNORE_CASE_OPTION = "i";
  private static final String OPTIONS = "$options";
  private static final String REGEX = "$regex";

  private static final MongoSelectTypeExpressionParser identifierParser =
      new MongoIdentifierExpressionParser();
  // Only a constant RHS is supported as of now
  private static final MongoSelectTypeExpressionParser rhsParser =
      new MongoConstantExpressionParser();

  @Override
  public Map<String, Object> apply(final SelectTypeExpression lhs, final SelectTypeExpression rhs) {
    final String parsedLhs = lhs.accept(identifierParser);
    final Object parsedRhs = rhs.accept(rhsParser);
    return Map.of(
        parsedLhs, new BasicDBObject(REGEX, parsedRhs).append(OPTIONS, IGNORE_CASE_OPTION));
  }
}
