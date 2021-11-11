package org.hypertrace.core.documentstore.mongo.parser;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.parser.SortingExpressionParser;
import org.hypertrace.core.documentstore.query.SortingDefinition;

public class MongoSortingExpressionParser implements SortingExpressionParser {

  private static final String SORT_CLAUSE = "$sort";

  private final SortingOrder order;

  protected MongoSortingExpressionParser(final SortingOrder order) {
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
        String.format("Cannot sort a function ($%s) in MongoDB.",
            expression.getOperator().name().toLowerCase()));
  }

  @Override
  public Map<String, Object> parse(final IdentifierExpression expression) {
    String parsed = new MongoIdentifierExpressionParser().parse(expression);
    return Map.of(parsed, getOrder());
  }

  public static BasicDBObject getSortClause(List<SortingDefinition> sortingDefinitions) {
    if (CollectionUtils.isEmpty(sortingDefinitions)) {
      return new BasicDBObject();
    }

    return new BasicDBObject(SORT_CLAUSE, getOrders(sortingDefinitions));
  }

  public static void applySorting(FindIterable<BasicDBObject> iterable,
      List<SortingDefinition> sortingDefinitions) {
    if (CollectionUtils.isEmpty(sortingDefinitions)) {
      return;
    }

    iterable.sort(new BasicDBObject(getOrders(sortingDefinitions)));
  }

  private static Map<String, Object> getOrders(List<SortingDefinition> sortingDefinitions) {
    return sortingDefinitions
        .stream()
        .map(MongoSortingExpressionParser::parse)
        .reduce(new LinkedHashMap<>(), (first, second) -> {
          first.putAll(second);
          return first;
        });
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

  private static Map<String, Object> parse(SortingDefinition definition) {
    MongoSortingExpressionParser parser = new MongoSortingExpressionParser(definition.getOrder());
    return (Map<String, Object>) definition.getExpression().parse(parser);
  }
}
