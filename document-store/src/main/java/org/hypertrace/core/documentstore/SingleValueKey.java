package org.hypertrace.core.documentstore;

import java.util.Objects;

public class SingleValueKey implements Key {

  private final String tenantId;
  private final String value;

  public SingleValueKey(String tenantId, String value) {
    this.tenantId = tenantId;
    this.value = value;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("%s:%s", tenantId, value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }

    return Objects.equals(((SingleValueKey) o).value, this.value)
        && Objects.equals(((SingleValueKey) o).tenantId, this.tenantId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, tenantId);
  }
}
