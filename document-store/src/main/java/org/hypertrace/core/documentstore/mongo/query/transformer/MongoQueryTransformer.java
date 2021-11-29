package org.hypertrace.core.documentstore.mongo.query.transformer;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.hypertrace.core.documentstore.query.QueryInternal;
import org.hypertrace.core.documentstore.query.transform.QueryTransformer;

public class MongoQueryTransformer {
  // Order of transformation application matters
  private static final List<QueryTransformer> TRANSFORMERS =
      new ImmutableList.Builder<QueryTransformer>()
          .add(new MongoSelectionsAddingTransformer())
          .add(new MongoSelectionsUpdatingTransformer())
          .build();

  public static QueryInternal transform(final QueryInternal query) {
    QueryInternal transformedQuery = query;

    for (QueryTransformer transformer : TRANSFORMERS) {
      transformedQuery = transformer.transform(transformedQuery);
    }

    return transformedQuery;
  }
}
