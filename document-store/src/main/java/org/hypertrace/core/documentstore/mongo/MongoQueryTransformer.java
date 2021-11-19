package org.hypertrace.core.documentstore.mongo;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public class MongoQueryTransformer {

  static void transform(final Query query) {
    // Make sure that all the grouping statements (AggregateExpressions) are included in the
    // final projections
    addProjectionsForAggregations(query);
  }

  private static void addProjectionsForAggregations(final Query query) {
    List<SelectionSpec> specs = query.getSelections();

    if (CollectionUtils.isEmpty(specs)) {
      return;
    }

    List<SelectionSpec> pairingSpecs =
        specs.stream()
            .map(MongoQueryTransformer::getPairingSelectionForAggregations)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    specs.addAll(pairingSpecs);
  }

  private static SelectionSpec getPairingSelectionForAggregations(final SelectionSpec spec) {
    if (!(spec.getExpression() instanceof AggregateExpression)) {
      return null;
    }

    AggregateExpression expression = (AggregateExpression) spec.getExpression();
    String alias = spec.getAlias();

    if (expression.getAggregator() == DISTINCT_COUNT) {
      // Since MongoDB doesn't support $distinctCount in aggregations, we convert this to
      // $addToSet function. So, we need to project $size(set) instead of just the alias
      SelectingExpression pairingExpression =
          FunctionExpression.builder()
              .operator(LENGTH)
              .operand(IdentifierExpression.of(alias))
              .build();
      return SelectionSpec.of(pairingExpression, alias);
    }

    return SelectionSpec.of(IdentifierExpression.of(alias));
  }
}
