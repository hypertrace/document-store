package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;

import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
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

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final KeyExpression expression) {
    return Map.of(ID_KEY, expression.getKey().toString());
  }

  public static BasicDBObject getFilterClause(
      final Query query, final Function<Query, Optional<FilterTypeExpression>> filterProvider) {
    BasicDBObject filters = getFilter(query, filterProvider);
    return convertFilterToClause(filters);
  }

  public static BasicDBObject getFilterClause(FilterTypeExpression filterTypeExpression) {
    BasicDBObject filters = getFilter(filterTypeExpression);
    return convertFilterToClause(filters);
  }

  public static BasicDBObject getFilter(
      final Query query, final Function<Query, Optional<FilterTypeExpression>> filterProvider) {
    Optional<FilterTypeExpression> filterOptional = filterProvider.apply(query);

    if (filterOptional.isEmpty()) {
      return new BasicDBObject();
    }

    return getFilter(filterOptional.get());
  }

  private static BasicDBObject convertFilterToClause(BasicDBObject filters) {
    return filters.isEmpty() ? new BasicDBObject() : new BasicDBObject(FILTER_CLAUSE, filters);
  }

  private static BasicDBObject getFilter(final FilterTypeExpression filterTypeExpression) {
    final FilterTypeExpressionVisitor parser = new MongoFilterTypeExpressionParser();
    final Map<String, Object> filter = filterTypeExpression.accept(parser);
    return new BasicDBObject(filter);
  }
}
