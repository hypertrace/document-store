package org.hypertrace.core.documentstore.mongo.query.transformer;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.documentstore.query.QueryInternal;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.transform.QueryTransformer;
import org.hypertrace.core.documentstore.query.transform.TransformedQueryBuilder;

final class MongoSelectionsUpdatingTransformer implements QueryTransformer {
  @Override
  public QueryInternal transform(final QueryInternal query) {
    List<SelectionSpec> newSpecs = new ArrayList<>();

    for (SelectionSpec spec : query.getSelections()) {
      MongoSelectionsUpdatingTransformation transformer =
          new MongoSelectionsUpdatingTransformation(query.getAggregations(), spec);
      SelectionSpec newSpec = spec.getExpression().visit(transformer);
      newSpecs.add(newSpec);
    }

    return (QueryInternal) new TransformedQueryBuilder(query).setSelections(newSpecs).build();
  }
}
