package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.parser.FilteringExpressionParser;

public class MongoFilteringExpressionParser implements FilteringExpressionParser {

  private static final String FILTER_CLAUSE = "$match";

  @Override
  public Map<String, Object> parse(final LogicalExpression expression) {
    return new MongoLogicalExpressionParser().parse(expression);
  }

  @Override
  public Map<String, Object> parse(final RelationalExpression expression) {
    return new MongoRelationalExpressionParser().parse(expression);
  }

  public static BasicDBObject getFilterClause(FilteringExpression expression) {
    Object filter;

    if (expression == null) {
      filter = Map.of();
    } else {
      filter = expression.parse(new MongoFilteringExpressionParser());
    }

    return new BasicDBObject(FILTER_CLAUSE, filter);
  }
}
