package org.hypertrace.core.documentstore.mongo.expression.transformer;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;

import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.mongo.MongoCollection;
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
@AllArgsConstructor
public class MongoSelectionsUpdatingTransformation implements SelectingExpressionVisitor {
  private final List<GroupingExpression> aggregations;
  private final SelectionSpec source;

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(AggregateExpression expression) {
    if (expression.getAggregator() == DISTINCT_COUNT) {
      return SelectionSpec.of(
          AggregateExpression.of(DISTINCT, expression.getExpression()), source.getAlias());
    }

    return source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(ConstantExpression expression) {
    return source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(FunctionExpression expression) {
    return source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SelectionSpec visit(IdentifierExpression expression) {
    for (GroupingExpression group : aggregations) {
      if (expression.equals(group)) {
        String key = expression.getName();
        String identifier = MongoCollection.ID_KEY + "." + key;

        return SelectionSpec.of(IdentifierExpression.of(identifier), key);
      }
    }

    return source;
  }
}
