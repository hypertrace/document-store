package org.hypertrace.core.documentstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Query {

  // support only filter for now. Add aggregations and group by later.
  private final List<String> selections = new ArrayList<>();
  private Filter filter;
  private final List<OrderBy> orderBys = new ArrayList<>();
  private Integer offset;
  private Integer limit;

  public void addAllSelections(List<String> selections) {
    this.selections.addAll(selections);
  }

  public void addSelection(String selection) {
    this.selections.add(selection);
  }

  public List<String> getSelections() {
    return selections;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public void addAllOrderBys(List<OrderBy> orderBys) {
    this.orderBys.addAll(orderBys);
  }

  public void addOrderBy(OrderBy orderBy) {
    this.orderBys.add(orderBy);
  }

  public List<OrderBy> getOrderBys() {
    return orderBys;
  }

  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  @Override
  public String toString() {
    return "Query{"
        + "selections="
        + selections
        + ", filter="
        + filter
        + ", orderBys="
        + orderBys
        + ", offset="
        + offset
        + ", limit="
        + limit
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Query query = (Query) o;
    return Objects.equals(selections, query.selections)
        && Objects.equals(filter, query.filter)
        && Objects.equals(orderBys, query.orderBys)
        && Objects.equals(offset, query.offset)
        && Objects.equals(limit, query.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(selections, filter, orderBys, offset, limit);
  }
}
