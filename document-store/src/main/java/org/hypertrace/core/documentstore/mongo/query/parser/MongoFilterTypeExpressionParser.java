package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;

import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public final class MongoFilterTypeExpressionParser implements FilterTypeExpressionVisitor {

  private static final String FILTER_CLAUSE = "$match";

  private final UnaryOperator<MongoSelectTypeExpressionParser> wrappingLhsParser;

  public MongoFilterTypeExpressionParser() {
    this(UnaryOperator.identity());
  }

  public MongoFilterTypeExpressionParser(
      final UnaryOperator<MongoSelectTypeExpressionParser> wrappingLhsParser) {
    this.wrappingLhsParser = wrappingLhsParser;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final LogicalExpression expression) {
    return new MongoLogicalExpressionParser(wrappingLhsParser).parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final RelationalExpression expression) {
    return new MongoRelationalExpressionParser(wrappingLhsParser).parse(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final KeyExpression expression) {
    return expression.getKeys().size() == 1
        ? Map.of(ID_KEY, expression.getKeys().get(0).toString())
        : Map.of(
            ID_KEY,
            Map.of("$in", expression.getKeys().stream().map(Key::toString).toArray(String[]::new)));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final ArrayRelationalFilterExpression expression) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final DocumentArrayFilterExpression expression) {
    throw new UnsupportedOperationException();
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
