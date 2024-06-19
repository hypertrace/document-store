package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.ORDER_MAP;
import static org.hypertrace.core.documentstore.mongo.query.parser.MongoSortTypeExpressionParser.SORT_CLAUSE;

import com.mongodb.BasicDBObject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
    MongoNonProjectedSortTypeExpressionParser parser =
        new MongoNonProjectedSortTypeExpressionParser(definition.getOrder());
    Map<String, SelectionSpec> aliasToSelectionMap =
        selectionSpecs.stream()
            .filter(spec -> MongoNonProjectedSortTypeExpressionParser.getAlias(spec) != null)
            .collect(
                Collectors.toUnmodifiableMap(
                    MongoNonProjectedSortTypeExpressionParser::getAlias, Function.identity()));

    String sortAlias = definition.getExpression().accept(new AliasParser());
    SelectionSpec sortSelectionSpec = aliasToSelectionMap.get(sortAlias);
    // If selection spec is present for the sort expression,
    // use the selection spec for sort order, else do nothing
    if (sortSelectionSpec != null) {
      return sortSelectionSpec.getExpression().accept(parser);
    }

    return definition.getExpression().accept(parser);
  }

  private static String getAlias(SelectionSpec selectionSpec) {
    if (selectionSpec.getAlias() != null) {
      return selectionSpec.getAlias();
    }

    return selectionSpec.getExpression().accept(new AliasParser());
  }
}
