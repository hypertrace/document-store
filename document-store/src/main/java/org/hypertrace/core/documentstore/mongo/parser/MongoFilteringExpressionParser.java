package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.parser.FilteringExpressionParser;
import org.hypertrace.core.documentstore.query.Query;

public class MongoFilteringExpressionParser extends MongoExpressionParser
    implements FilteringExpressionParser {

  private static final String FILTER_CLAUSE = "$match";

  protected MongoFilteringExpressionParser(Query query) {
    super(query);
  }

  @Override
  public Map<String, Object> parse(final LogicalExpression expression) {
    return new MongoLogicalExpressionParser(query).parse(expression);
  }

  @Override
  public Map<String, Object> parse(final RelationalExpression expression) {
    return new MongoRelationalExpressionParser(query).parse(expression);
  }

  public static BasicDBObject getFilterClause(
      final Query query, final Function<Query, Optional<FilteringExpression>> filterProvider) {
    BasicDBObject filters = getFilter(query, filterProvider);
    return filters.isEmpty() ? new BasicDBObject() : new BasicDBObject(FILTER_CLAUSE, filters);
  }

  @SuppressWarnings("unchecked")
  public static BasicDBObject getFilter(
      final Query query, final Function<Query, Optional<FilteringExpression>> filterProvider) {
    Optional<FilteringExpression> filterOptional = filterProvider.apply(query);

    if (filterOptional.isEmpty()) {
      return new BasicDBObject();
    }

    FilteringExpressionParser parser = new MongoFilteringExpressionParser(query);
    Map<String, Object> filter = (Map<String, Object>) filterOptional.get().parse(parser);
    return new BasicDBObject(filter);
  }
}
