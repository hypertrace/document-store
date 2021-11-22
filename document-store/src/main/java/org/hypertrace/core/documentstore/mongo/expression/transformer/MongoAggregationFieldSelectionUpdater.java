package org.hypertrace.core.documentstore.mongo.expression.transformer;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Query.TransformedQueryBuilder;
import org.hypertrace.core.documentstore.query.QueryTransformer;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public class MongoAggregationFieldSelectionUpdater implements QueryTransformer {

  @Override
  public Query transform(final Query query) {
    List<SelectionSpec> newSpecs = new ArrayList<>();

    for (SelectionSpec spec : query.getSelections()) {
      MongoAggregationFieldSelectionUpdatingTransformer transformer =
          new MongoAggregationFieldSelectionUpdatingTransformer(query.getAggregations(), spec);
      SelectionSpec newSpec = spec.getExpression().visit(transformer);
      newSpecs.add(newSpec);
    }

    return new TransformedQueryBuilder(query).buildWithSelections(newSpecs);
  }
}
