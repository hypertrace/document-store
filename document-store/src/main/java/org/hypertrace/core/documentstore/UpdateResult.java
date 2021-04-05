package org.hypertrace.core.documentstore;

/*
 * Represent the result object for UPDATE operation of document store APIs.
 * */
public class UpdateResult {
  private long updatedCount;

  public UpdateResult(long updatedCount) {
    this.updatedCount = updatedCount;
  }

  public long getUpdatedCount() {
    return updatedCount;
  }
}
