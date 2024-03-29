package org.hypertrace.core.documentstore.mongo.query.transformer;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.transform.QueryTransformer;
import org.hypertrace.core.documentstore.query.transform.TransformedQueryBuilder;

final class MongoSelectionsUpdatingTransformer implements QueryTransformer {
  @Override
  public Query transform(final Query query) {
    List<SelectionSpec> newSpecs = new ArrayList<>();

    for (SelectionSpec spec : query.getSelections()) {
      MongoSelectionsUpdatingTransformation transformer =
          new MongoSelectionsUpdatingTransformation(query.getAggregations(), spec);
      SelectionSpec newSpec = spec.getExpression().accept(transformer);
      newSpecs.add(newSpec);
    }

    return new TransformedQueryBuilder(query).setSelections(newSpecs).build();
  }
}
