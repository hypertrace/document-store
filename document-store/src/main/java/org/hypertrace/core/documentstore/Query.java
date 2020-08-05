package org.hypertrace.core.documentstore;

import java.util.ArrayList;
import java.util.List;

public class Query {

  // support only filter for now. Add projections and aggregations and group by later.
  private Filter filter;
  private List<OrderBy> orderBys = new ArrayList<>();
  private Integer offset;
  private Integer limit;

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public void setOrderBy(OrderBy orderBy) {
    orderBys.add(orderBy);
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
    if (filter == null) {
      return new Filter().toString();
    } else {
      return filter.toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Query that = (Query) o;
    return filter.equals(that.filter) && orderBys.equals(that.orderBys);
  }
}
