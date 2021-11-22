package org.hypertrace.core.documentstore.mongo;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.hypertrace.core.documentstore.mongo.expression.transformer.MongoSelectionsAddingTransformer;
import org.hypertrace.core.documentstore.mongo.expression.transformer.MongoSelectionsUpdatingTransformer;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.QueryTransformer;

public class MongoQueryTransformer {
  // Order of transformation application matters
  private static final List<QueryTransformer> TRANSFORMERS =
      new ImmutableList.Builder<QueryTransformer>()
          .add(new MongoSelectionsAddingTransformer())
          .add(new MongoSelectionsUpdatingTransformer())
          .build();

  static Query transform(final Query query) {
    Query transformedQuery = query;

    for (QueryTransformer transformer : TRANSFORMERS) {
      transformedQuery = transformer.transform(transformedQuery);
    }

    return transformedQuery;
  }
}
