package org.hypertrace.core.documentstore.model;

public interface Hashable {
  @Override
  int hashCode();

  @Override
  boolean equals(final Object object);
}
