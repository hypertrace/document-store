package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.parser.SortingExpressionParser;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SortingSpec;

public class MongoSortingExpressionParser extends MongoExpressionParser
    implements SortingExpressionParser {

  private static final String SORT_CLAUSE = "$sort";

  private final SortingOrder order;

  protected MongoSortingExpressionParser(final Query query, final SortingOrder order) {
    super(query);
    this.order = order;
  }

  @Override
  public Map<String, Object> parse(final AggregateExpression expression) {
    throw new UnsupportedOperationException(
        String.format(
            "Cannot sort an aggregation ($%s) in MongoDB. "
                + "Set alias in selection and sort by the alias as identifier",
            expression.getAggregator().name().toLowerCase()));
  }

  @Override
  public Map<String, Object> parse(final FunctionExpression expression) {
    throw new UnsupportedOperationException(
        String.format(
            "Cannot sort a function ($%s) in MongoDB.",
            expression.getOperator().name().toLowerCase()));
  }

  @Override
  public Map<String, Object> parse(final IdentifierExpression expression) {
    String parsed = new MongoIdentifierExpressionParser(query).parse(expression);
    return Map.of(parsed, getOrder());
  }

  public static BasicDBObject getSortClause(final Query query) {
    BasicDBObject orders = getOrders(query);
    return orders.isEmpty() ? orders : new BasicDBObject(SORT_CLAUSE, orders);
  }

  public static BasicDBObject getOrders(final Query query) {
    List<SortingSpec> sortingSpecs = query.getSorts();

    if (CollectionUtils.isEmpty(sortingSpecs)) {
      return new BasicDBObject();
    }

    Map<String, Object> map =
        sortingSpecs.stream()
            .map(spec -> MongoSortingExpressionParser.parse(query, spec))
            .reduce(
                new LinkedHashMap<>(),
                (first, second) -> {
                  first.putAll(second);
                  return first;
                });

    return new BasicDBObject(map);
  }

  private int getOrder() {
    switch (order) {
      case ASC:
        return 1;
      case DESC:
        return -1;
    }

    throw new IllegalArgumentException("Unknown sorting order: " + order.name());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parse(final Query query, final SortingSpec definition) {
    MongoSortingExpressionParser parser =
        new MongoSortingExpressionParser(query, definition.getOrder());
    return (Map<String, Object>) definition.getExpression().parse(parser);
  }
}
