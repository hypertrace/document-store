package org.hypertrace.core.documentstore.mongo;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.hypertrace.core.documentstore.mongo.expression.transformer.MongoAggregateExpressionSelectionAdder;
import org.hypertrace.core.documentstore.mongo.expression.transformer.MongoAggregationFieldSelectionUpdater;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.QueryTransformer;

public class MongoQueryTransformer {
  private static final List<QueryTransformer> TRANSFORMERS =
      new ImmutableList.Builder<QueryTransformer>()
          .add(new MongoAggregationFieldSelectionUpdater())
          .add(new MongoAggregateExpressionSelectionAdder())
          .build();

  static Query transform(final Query query) {
    Query transformedQuery = query;

    for (QueryTransformer transformer : TRANSFORMERS) {
      transformedQuery = transformer.transform(transformedQuery);
    }

    return transformedQuery;

    //    // Expose any identifier used in the aggregation since "_id" is not exposed in the
    // result-set
    //    modifyProjectionsBasedOnAggregations(query);
    //
    //    // Make sure that all the grouping statements (AggregateExpressions) are included in the
    //    // final projections
    //    addProjectionsForAggregations(query);
  }
  //
  //  private static void addProjectionsForAggregations(final Query query) {
  //    List<SelectionSpec> specs = query.getSelections();
  //
  //    if (CollectionUtils.isEmpty(specs)) {
  //      return;
  //    }
  //
  //    List<SelectionSpec> pairingSpecs =
  //        specs.stream()
  //            .map(MongoQueryTransformer::getPairingSelectionForAggregations)
  //            .filter(Objects::nonNull)
  //            .collect(Collectors.toList());
  //
  //    specs.addAll(pairingSpecs);
  //  }
  //
  //  private static void modifyProjectionsBasedOnAggregations(final Query query) {
  //    List<Integer> updateIndexes = new ArrayList<>();
  //    List<SelectionSpec> newSpecs = new ArrayList<>();
  //    List<SelectionSpec> oldSpecs = query.getSelections();
  //
  //    for (int i = 0; i < oldSpecs.size(); i++) {
  //      SelectionSpec spec = oldSpecs.get(i);
  //      SelectingExpression expression = spec.getExpression();
  //
  //      if (!(expression instanceof IdentifierExpression)) {
  //        continue;
  //      }
  //
  //      IdentifierExpression idExpression = (IdentifierExpression) expression;
  //
  //      for (GroupingExpression group : query.getAggregations()) {
  //        if (idExpression.equals(group)) {
  //          String key = idExpression.getName();
  //          String identifier = MongoCollection.ID_KEY + "." + key;
  //
  //          newSpecs.add(SelectionSpec.of(IdentifierExpression.of(identifier), key));
  //          updateIndexes.add(i);
  //        }
  //      }
  //    }
  //
  //    for (int i = updateIndexes.size() - 1; i >= 0; i--) {
  //      int index = updateIndexes.get(i);
  //      oldSpecs.remove(index);
  //      oldSpecs.add(index, newSpecs.get(i));
  //    }
  //  }
  //
  //  private static SelectionSpec getPairingSelectionForAggregations(final SelectionSpec spec) {
  //    if (!(spec.getExpression() instanceof AggregateExpression)) {
  //      return null;
  //    }
  //
  //    AggregateExpression expression = (AggregateExpression) spec.getExpression();
  //    String alias = spec.getAlias();
  //
  //    if (expression.getAggregator() == DISTINCT_COUNT) {
  //      // Since MongoDB doesn't support $distinctCount in aggregations, we convert this to
  //      // $addToSet function. So, we need to project $size(set) instead of just the alias
  //      SelectingExpression pairingExpression =
  //          FunctionExpression.builder()
  //              .operator(LENGTH)
  //              .operand(IdentifierExpression.of(alias))
  //              .build();
  //      return SelectionSpec.of(pairingExpression, alias);
  //    }
  //
  //    return SelectionSpec.of(IdentifierExpression.of(alias));
  //  }
}
