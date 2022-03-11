package org.hypertrace.core.documentstore;

public class DeleteResult {
  private long deletedCount;

  public DeleteResult(long deletedCount) {
    this.deletedCount = deletedCount;
  }

  public long getDeletedCount() {
    return deletedCount;
  }
}
