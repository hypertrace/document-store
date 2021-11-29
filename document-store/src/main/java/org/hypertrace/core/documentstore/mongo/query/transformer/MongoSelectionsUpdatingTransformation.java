package org.hypertrace.core.documentstore.mongo.query.transformer;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.FIELD_SEPARATOR;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.encodeKey;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.SelectionSpec;

/**
 * The objective of this class is to update any of the existing selections.
 *
 * <p>Current implementation contains (a) prefix any aggregate selections with "_id." so that the
 * field is extracted out in the response. (b) convert any unsupported aggregation operations to
 * supported aggregation operations.
 *
 * <p>For example, this transformation converts the aggregate pipeline <code>
 *   [
 *      {
 *        "$group": {
 *          "_id": {
 *            "item": "$item",
 *            "price": "$props.price"
 *          },
 *          "num_brands": {
 *            "$distinctCount": "$brand"
 *          }
 *        }
 *      },
 *      {
 *        "$project": {
 *          "item": 1
 *        }
 *      }
 *   ]
 * </code> into <code>
 *   [
 *      {
 *        "$group": {
 *          "_id": {
 *            "item": "$item",
 *            "price": "$props.price"
 *          },
 *          "num_brands": {
 *            "$distinct": "$brand"
 *          }
 *        }
 *      },
 *      {
 *        "$project": {
 *          "item": "$_id.item"
 *        }
 *      }
 *   ]
 * </code> since "item" appears in projection as well as grouping and "$distinctCount" is not
 * supported
 */
final class MongoSelectionsUpdatingTransformation implements SelectingExpressionVisitor {
  private static final Function<AggregateExpression, AggregateExpression> COUNT_HANDLER =
      expression -> AggregateExpression.of(SUM, ConstantExpression.of(1));

  private static final Function<AggregateExpression, AggregateExpression> DISTINCT_COUNT_HANDLER =
      expression -> AggregateExpression.of(DISTINCT, expression.getExpression());

  private static final Map<AggregationOperator, Function<AggregateExpression, AggregateExpression>>
      AGGREGATION_SUBSTITUTE_MAP =
          unmodifiableMap(
              new EnumMap<>(AggregationOperator.class) {
                {
                  put(DISTINCT_COUNT, DISTINCT_COUNT_HANDLER);
                  put(COUNT, COUNT_HANDLER);
                }
              });

  private final List<GroupingExpression> groupingExpressions;
  private final SelectionSpec source;

  MongoSelectionsUpdatingTransformation(
      List<GroupingExpression> groupingExpressions, SelectionSpec source) {
    this.groupingExpressions = groupingExpressions;
    this.source = source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(final AggregateExpression expression) {
    return substitute(expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(final ConstantExpression expression) {
    return source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(final FunctionExpression expression) {
    return source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(final IdentifierExpression expression) {
    GroupingExpression matchingGroup = null;

    for (final GroupingExpression group : groupingExpressions) {
      if (expression.equals(group)) {
        matchingGroup = group;
        break;
      }
    }

    if (matchingGroup == null) {
      return source;
    }

    String key = expression.getName();
    String identifier = ID_KEY + FIELD_SEPARATOR + encodeKey(key);
    String alias = Optional.ofNullable(source.getAlias()).orElse(key);

    return SelectionSpec.of(IdentifierExpression.of(identifier), alias);
  }

  private SelectionSpec substitute(final AggregateExpression expression) {
    return Optional.ofNullable(AGGREGATION_SUBSTITUTE_MAP.get(expression.getAggregator()))
        .map(converter -> SelectionSpec.of(converter.apply(expression), source.getAlias()))
        .orElse(source);
  }
}
