package org.hypertrace.core.documentstore.mongo.query.parser;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import com.mongodb.BasicDBObject;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.SortingSpec;

/**
 * Parser to parse projected sort type expressions into their respective fields names defined in
 * selections
 *
 * <p>Example: SortBy ("attributeName", DESC) with respective projection as Selection(alias:
 * "attributeName", key: "attribute.name") would result in sort expression as
 * sortBy("attribute.name", DESC)
 */
public class MongoNonProjectedSortTypeExpressionParser implements SortTypeExpressionVisitor {

  private static final String SORT_CLAUSE = "$sort";
  private static final Map<SortOrder, Integer> ORDER_MAP =
      unmodifiableMap(
          new EnumMap<>(SortOrder.class) {
            {
              put(ASC, 1);
              put(DESC, -1);
            }
          });

  private final SortOrder order;

  MongoNonProjectedSortTypeExpressionParser(final SortOrder order) {
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
    Integer value = ORDER_MAP.get(order);

    if (value == null) {
      throw getUnsupportedOperationException(order);
    }

    String parsed = new MongoIdentifierExpressionParser().parse(expression);
    return Map.of(parsed, value);
  }

  public static BasicDBObject getNonProjectedSortClause(final Query query) {
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
            .map(sortingSpec -> parse(sortingSpec, query.getSelections()))
            .reduce(
                new LinkedHashMap<>(),
                (first, second) -> {
                  first.putAll(second);
                  return first;
                });

    return new BasicDBObject(map);
  }

  private static Map<String, Object> parse(
      final SortingSpec definition, final List<SelectionSpec> selectionSpecs) {
    if (!definition.getExpression().getClass().equals(IdentifierExpression.class)) {
      // This parser is only meant to convert identifier based sort
      // expression to their projected query. If you have sort on other
      // functions or aggregations, then use MongoSortTypeExpressionParser
      throw new IllegalArgumentException("Sort expression should be an identifier");
    }

    MongoNonProjectedSortTypeExpressionParser parser =
        new MongoNonProjectedSortTypeExpressionParser(definition.getOrder());
    Map<String, List<SelectionSpec>> aliasToSelectionMap =
        selectionSpecs.stream()
            .collect(
                Collectors.groupingBy(
                    MongoNonProjectedSortTypeExpressionParser::getAlias, toList()));

    List<SelectionSpec> sortSelectionSpec =
        aliasToSelectionMap.get(((IdentifierExpression) definition.getExpression()).getName());

    // no selection spec is present for the sort expression
    if (sortSelectionSpec == null || sortSelectionSpec.isEmpty()) {
      return definition.getExpression().accept(parser);
    }

    return ((IdentifierExpression) sortSelectionSpec.get(0).getExpression()).accept(parser);
  }

  private static String getAlias(SelectionSpec selectionSpec) {
    return selectionSpec.getAlias() != null
        ? selectionSpec.getAlias()
        : ((IdentifierExpression) selectionSpec.getExpression()).getName();
  }
}
