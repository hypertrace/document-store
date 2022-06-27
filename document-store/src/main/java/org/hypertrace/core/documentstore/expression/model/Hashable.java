package org.hypertrace.core.documentstore.expression.model;

public interface Hashable {
  @Override
  int hashCode();

  @Override
  boolean equals(final Object object);
}
