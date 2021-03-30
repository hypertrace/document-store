package org.hypertrace.core.documentstore;

/*
 * Represent the base result for all the newly added document store APIs.
 *
 * */
public class DocStoreResult {
  private boolean success;
  private long resultCount;
  private String failureMessage;

  public DocStoreResult(Builder builder) {
    this.success = builder.success;
    this.resultCount = builder.resultCount;
    this.failureMessage = builder.failureMessage;
  }

  /** Return True if the operation was successful */
  public boolean isSuccess() {
    return success;
  }

  /** Returns the number of documents successfully inserted or updated */
  public long getResultCount() {
    return resultCount;
  }

  /** Additional message in case of operation failed */
  public String getFailureMessage() {
    return failureMessage;
  }

  public static class Builder {
    private boolean success;
    private long resultCount;
    private String failureMessage;

    private Builder() {}

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder setSuccess(boolean success) {
      this.success = success;
      return this;
    }

    public Builder setResultCount(long resultCount) {
      this.resultCount = resultCount;
      return this;
    }

    public Builder setFailureMessage(String failureMessage) {
      this.failureMessage = failureMessage;
      return this;
    }

    public DocStoreResult build() {
      return new DocStoreResult(this);
    }
  }
}
