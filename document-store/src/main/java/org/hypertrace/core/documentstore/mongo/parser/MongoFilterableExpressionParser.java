package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterableExpression;
import org.hypertrace.core.documentstore.parser.FilterableExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public final class MongoFilterableExpressionParser implements FilterableExpressionVisitor {

  private static final String FILTER_CLAUSE = "$match";

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final LogicalExpression expression) {
    return new MongoLogicalExpressionMongoParser().parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final RelationalExpression expression) {
    return new MongoRelationalExpressionParser().parse(expression);
  }

  public static BasicDBObject getFilterClause(
      final Query query, final Function<Query, Optional<FilterableExpression>> filterProvider) {
    BasicDBObject filters = getFilter(query, filterProvider);
    return filters.isEmpty() ? new BasicDBObject() : new BasicDBObject(FILTER_CLAUSE, filters);
  }

  public static BasicDBObject getFilter(
      final Query query, final Function<Query, Optional<FilterableExpression>> filterProvider) {
    Optional<FilterableExpression> filterOptional = filterProvider.apply(query);

    if (filterOptional.isEmpty()) {
      return new BasicDBObject();
    }

    FilterableExpressionVisitor parser = new MongoFilterableExpressionParser();
    Map<String, Object> filter = filterOptional.get().accept(parser);
    return new BasicDBObject(filter);
  }
}
