package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.SortingOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortingOrder.DESC;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import com.mongodb.BasicDBObject;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.parser.SortingExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SortingSpec;

public final class MongoSortingExpressionParser implements SortingExpressionVisitor {

  private static final String SORT_CLAUSE = "$sort";
  private static final Map<SortingOrder, Integer> ORDER_MAP =
      unmodifiableMap(
          new EnumMap<>(SortingOrder.class) {
            {
              put(ASC, 1);
              put(DESC, -1);
            }
          });

  private final SortingOrder order;

  MongoSortingExpressionParser(final SortingOrder order) {
    this.order = order;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final AggregateExpression expression) {
    throw new UnsupportedOperationException(
        String.format(
            "Cannot sort an aggregation ($%s) in MongoDB. "
                + "Set alias in selection and sort by the alias as identifier",
            expression.getAggregator().name().toLowerCase()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final FunctionExpression expression) {
    throw new UnsupportedOperationException(
        String.format(
            "Cannot sort a function ($%s) in MongoDB."
                + "Set alias in selection and sort by the alias as identifier",
            expression.getOperator().name().toLowerCase()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final IdentifierExpression expression) {
    String parsed = new MongoIdentifierExpressionParser().parse(expression);
    Integer value = ORDER_MAP.get(order);

    if (value == null) {
      throw getUnsupportedOperationException(order);
    }

    return Map.of(parsed, value);
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
            .map(MongoSortingExpressionParser::parse)
            .reduce(
                new LinkedHashMap<>(),
                (first, second) -> {
                  first.putAll(second);
                  return first;
                });

    return new BasicDBObject(map);
  }

  private static Map<String, Object> parse(final SortingSpec definition) {
    MongoSortingExpressionParser parser = new MongoSortingExpressionParser(definition.getOrder());
    return definition.getExpression().visit(parser);
  }
}
