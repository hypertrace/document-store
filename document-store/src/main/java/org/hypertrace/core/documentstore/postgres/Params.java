package org.hypertrace.core.documentstore.postgres;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

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

  /** Wrapper class to hold array parameter metadata for PostgreSQL array binding */
  @Getter
  public static class ArrayParam {
    private final Object[] values;
    private final String sqlType;

    public ArrayParam(Object[] values, String sqlType) {
      this.values = values;
      this.sqlType = sqlType;
    }
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

    public Builder addArrayParam(Object[] values, String sqlType) {
      objectParams.put(nextIndex++, new ArrayParam(values, sqlType));
      return this;
    }

    public Params build() {
      return new Params(objectParams);
    }
  }
}
