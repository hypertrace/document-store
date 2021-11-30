package org.hypertrace.core.documentstore.query.transform;

import org.hypertrace.core.documentstore.query.Query;

public interface QueryTransformer {
  Query transform(final Query query);
}
