package org.hypertrace.core.documentstore;

import java.util.Arrays;
import java.util.Objects;

public class Filter {

  private Op op;
  private String fieldName;
  private Object value;
  private Filter[] childFilters;

  public enum Op {
    AND,
    OR,
    EQ,
    NEQ,
    GT,
    LT,
    GTE,
    LTE,
    IN,
    CONTAINS,
    EXISTS,
    NOT_EXISTS,
    LIKE,
    NOT_IN
  }

  public Filter() {}

  public Filter(Op op, String fieldName, Object value, Filter... childFilters) {
    this.op = op;
    this.fieldName = fieldName;
    this.value = value;
    this.childFilters = childFilters;
  }

  private Filter(Op op, Filter... childFilters) {
    this(op, null, null, childFilters);
  }

  public Op getOp() {
    return op;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Object getValue() {
    return value;
  }

  public void setOp(Op op) {
    this.op = op;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public void setChildFilters(Filter[] childFilters) {
    this.childFilters = childFilters;
  }

  public boolean isComposite() {
    return op == Op.AND || op == Op.OR;
  }

  public Filter[] getChildFilters() {
    return childFilters;
  }

  public static Filter eq(String fieldName, Object value) {
    return new Filter(Op.EQ, fieldName, value);
  }

  public Filter and(Filter... filters) {
    Filter[] childFilters = new Filter[filters.length + 1];
    int index = 0;
    childFilters[index++] = this;
    for (Filter filter : filters) {
      childFilters[index++] = filter;
    }

    return new Filter(Op.AND, childFilters);
  }

  public Filter or(Filter... filters) {
    Filter[] childFilters = new Filter[filters.length + 1];
    int index = 0;
    childFilters[index++] = this;
    for (Filter filter : filters) {
      childFilters[index++] = filter;
    }

    return new Filter(Op.OR, childFilters);
  }

  @Override
  public String toString() {
    return (fieldName != null ? fieldName + "-" : "")
        + (op != null ? op.name() + "-" : "")
        + (value != null ? value.toString() : "")
        + (childFilters != null ? toString(childFilters) : "");
  }

  private String toString(Filter[] childFilters) {
    if (childFilters.length == 0) return "";

    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (Filter child : childFilters) {
      sb.append(child.toString()).append(", ");
    }
    return sb.append("]").toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Filter filter = (Filter) o;
    return op == filter.op &&
        Objects.equals(fieldName, filter.fieldName) &&
        Objects.equals(value, filter.value) &&
        Arrays.equals(childFilters, filter.childFilters);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(op, fieldName, value);
    result = 31 * result + Arrays.hashCode(childFilters);
    return result;
  }
}
