package org.hypertrace.core.documentstore.mongo.query.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.transform.QueryTransformer;
import org.hypertrace.core.documentstore.query.transform.TransformedQueryBuilder;

final class MongoSelectionsAddingTransformer implements QueryTransformer {
  @Override
  public Query transform(final Query query) {
    List<SelectionSpec> newSpecs = new ArrayList<>();

    for (SelectionSpec spec : query.getSelections()) {
      String alias = spec.getAlias();
      SelectingExpression expression = spec.getExpression();

      MongoSelectionsAddingTransformation transformer =
          new MongoSelectionsAddingTransformation(alias);

      Optional<SelectionSpec> newSpec = expression.visit(transformer);
      newSpec.ifPresent(newSpecs::add);
    }

    return new TransformedQueryBuilder(query).addSelections(newSpecs).build();
  }
}
