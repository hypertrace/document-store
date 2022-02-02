package org.hypertrace.core.documentstore.mongo.query.transformer;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.encodeKey;

import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.SelectionSpec;

/**
 * The objective of this class is to introduce additional selections on top of the ones.
 *
 * <p>Current implementation contains adding projections to all the grouping fields (except "_id")
 *
 * <p>For example, this transformation converts the aggregate pipeline <code>
 *   [
 *      {
 *        "$group": {
 *          "_id": {
 *            "item": "$item",
 *          },
 *          "total": {
 *            "$sum": "$price"
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
 *          },
 *          "total": {
 *            "$sum": "$price"
 *          },
 *          "num_brands": {
 *            "$distinctCount": "$brand"
 *          }
 *        }
 *      },
 *      {
 *        "$project": {
 *          "item": 1,
 *          "total": 1,
 *          "num_brands": {
 *            "$size": "$num_brands"
 *          }
 *        }
 *      }
 *   ]
 * </code>
 */
@AllArgsConstructor
final class MongoSelectionsAddingTransformation implements SelectingExpressionVisitor {
  private final String alias;

  @SuppressWarnings("unchecked")
  @Override
  public Optional<SelectionSpec> visit(final AggregateExpression expression) {
    if (alias == null) {
      throw new IllegalArgumentException(
          String.format("Alias is must for projection: %s", expression));
    }

    final String encodedAlias = encodeKey(alias);
    final SelectingExpression pairingExpression;

    if (expression.getAggregator() == DISTINCT_COUNT) {
      // Since MongoDB doesn't support $distinctCount in aggregations, we convert this to
      // $addToSet function. So, we need to project $size(set) instead of just the alias
      pairingExpression =
          FunctionExpression.builder()
              .operator(LENGTH)
              .operand(IdentifierExpression.of(encodedAlias))
              .build();
    } else {
      pairingExpression = IdentifierExpression.of(encodedAlias);
    }

    return Optional.of(SelectionSpec.of(pairingExpression, alias));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<SelectionSpec> visit(final ConstantExpression expression) {
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<SelectionSpec> visit(final FunctionExpression expression) {
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<SelectionSpec> visit(final IdentifierExpression expression) {
    return Optional.empty();
  }
}
