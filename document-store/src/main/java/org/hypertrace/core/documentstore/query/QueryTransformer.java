package org.hypertrace.core.documentstore.query;

public interface QueryTransformer {
  Query transform(final Query query);
}
