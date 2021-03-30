package org.hypertrace.core.documentstore;

/*
 * Represent the result object for CREATE operation of document store APIs.
 * */
public class CreateResult {
  private long createdCount;

  public CreateResult(long createdCount) {
    this.createdCount = createdCount;
  }

  public long getCreatedCount() {
    return createdCount;
  }
}
