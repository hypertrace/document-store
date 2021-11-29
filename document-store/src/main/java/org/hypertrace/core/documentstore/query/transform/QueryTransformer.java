package org.hypertrace.core.documentstore.query.transform;

import org.hypertrace.core.documentstore.query.QueryInternal;

public interface QueryTransformer {
  QueryInternal transform(final QueryInternal query);
}
