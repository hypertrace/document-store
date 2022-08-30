package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.transform.QueryTransformer;

public class PostgresQueryTransformer {

  // Transform the query in the listed below order
  private static final List<QueryTransformer> TRANSFORMERS =
      new ImmutableList.Builder<QueryTransformer>()
          .add(new PostgresSelectionQueryTransformer())
          .build();

  public static Query transform(final Query query) {
    Query transformedQuery = query;

    for (QueryTransformer transformer : TRANSFORMERS) {
      transformedQuery = transformer.transform(transformedQuery);
    }

    return transformedQuery;
  }
}
