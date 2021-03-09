package org.hypertrace.core.documentstore.postgres;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds the params that need to be set in the PreparedStatement for constructing the final SQL
 * query
 */
public class Params {

  // Map of index to the corresponding param value
  private final Map<Integer, Object> objectParams;

  private Params(Map<Integer, Object> objectParams) {
    this.objectParams = objectParams;
  }

  @Override
  public String toString() {
    return "Params{" + "objectParams=" + objectParams + '}';
  }

  public Map<Integer, Object> getObjectParams() {
    return objectParams;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private int nextIndex;
    private final Map<Integer, Object> objectParams;

    private Builder() {
      nextIndex = 1;
      objectParams = new HashMap<>();
    }

    public Builder addObjectParam(Object paramValue) {
      objectParams.put(nextIndex++, paramValue);
      return this;
    }

    public Params build() {
      return new Params(objectParams);
    }
  }
}
