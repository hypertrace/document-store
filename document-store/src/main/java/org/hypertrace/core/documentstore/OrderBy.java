package org.hypertrace.core.documentstore;

import java.util.Objects;

public class OrderBy {

  private String field;
  private boolean isAsc;

  public OrderBy(String field, boolean isAsc) {
    this.field = field;
    this.isAsc = isAsc;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public boolean isAsc() {
    return isAsc;
  }

  public void setIsAsc(boolean isAsc) {
    this.isAsc = isAsc;
  }

  @Override
  public String toString() {
    return "OrderBy{" + "field='" + field + '\'' + ", isAsc=" + isAsc + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OrderBy orderBy = (OrderBy) o;
    return isAsc == orderBy.isAsc && Objects.equals(field, orderBy.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, isAsc);
  }
}
