package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public final class MongoFilterTypeExpressionParser implements FilterTypeExpressionVisitor {

  private static final String FILTER_CLAUSE = "$match";

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final LogicalExpression expression) {
    return new MongoLogicalExpressionParser().parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final RelationalExpression expression) {
    return new MongoRelationalExpressionParser().parse(expression);
  }

  public static BasicDBObject getFilterClause(
      final Query query, final Function<Query, Optional<FilterTypeExpression>> filterProvider) {
    BasicDBObject filters = getFilter(query, filterProvider);
    return filters.isEmpty() ? new BasicDBObject() : new BasicDBObject(FILTER_CLAUSE, filters);
  }

  public static BasicDBObject getFilter(
      final Query query, final Function<Query, Optional<FilterTypeExpression>> filterProvider) {
    Optional<FilterTypeExpression> filterOptional = filterProvider.apply(query);

    if (filterOptional.isEmpty()) {
      return new BasicDBObject();
    }

    FilterTypeExpressionVisitor parser = new MongoFilterTypeExpressionParser();
    Map<String, Object> filter = filterOptional.get().accept(parser);
    return new BasicDBObject(filter);
  }
}
