package org.hypertrace.core.documentstore;

public class BulkDeleteResult extends DeleteResult {

  public BulkDeleteResult(long deletedCount) {
    super(deletedCount);
  }
}
