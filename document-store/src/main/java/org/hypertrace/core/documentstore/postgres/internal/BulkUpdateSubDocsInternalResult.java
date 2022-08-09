package org.hypertrace.core.documentstore.postgres.internal;

import java.util.Set;
import org.hypertrace.core.documentstore.Key;

public class BulkUpdateSubDocsInternalResult {
  private Set<Key> updatedDocuments;
  private long totalUpdateCount;

  public BulkUpdateSubDocsInternalResult(Set<Key> updatedDocuments, long totalUpdateCount) {
    this.updatedDocuments = updatedDocuments;
    this.totalUpdateCount = totalUpdateCount;
  }

  public Set<Key> getUpdatedDocuments() {
    return updatedDocuments;
  }

  public long getTotalUpdateCount() {
    return totalUpdateCount;
  }
}
