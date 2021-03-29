package org.hypertrace.core.documentstore;

/*
 * Represent the base result for all the newly added document store APIs.
 * */
public class DocStoreResult {
  private boolean success;
  private long resultCount;
  private String message;

  public DocStoreResult(Builder builder) {
    this.success = builder.success;
    this.resultCount = builder.resultCount;
    this.message = builder.message;
  }

  public boolean isSuccess() {
    return success;
  }

  public long getResultCount() {
    return resultCount;
  }

  public String getMessage() {
    return message;
  }

  public static class Builder {
    private boolean success;
    private long resultCount;
    private String message;

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

    public Builder setMessage(String message) {
      this.message = message;
      return this;
    }

    public DocStoreResult build() {
      return new DocStoreResult(this);
    }
  }
}
