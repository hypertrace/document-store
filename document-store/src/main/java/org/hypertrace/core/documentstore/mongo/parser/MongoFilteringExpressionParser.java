package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.LogicalExpression;
import org.hypertrace.core.documentstore.expression.RelationalExpression;
import org.hypertrace.core.documentstore.parser.FilteringExpressionParser;

public class MongoFilteringExpressionParser implements FilteringExpressionParser {
  private static final String FILTER_CLAUSE = "$match";

  @Override
  public Object parse(LogicalExpression expression) {
    return null;
  }

  @Override
  public BasicDBObject parse(RelationalExpression expression) {
    Map<String, Object> parsedExpression = MongoRelationalExpressionParser.parse(expression);
    return new BasicDBObject(FILTER_CLAUSE, parsedExpression);
  }
}
