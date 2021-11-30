package org.hypertrace.core.documentstore.query;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
@ToString
public final class QueryInternal extends Query {
  private final Selection selection; // Missing selection represents fetching all the columns
  private final Filter filter;

  private final Aggregation aggregation;
  private final Filter aggregationFilter;

  private final Sort sort;
  private final Pagination pagination; // Missing pagination represents fetching all the records

  public List<SelectionSpec> getSelections() {
    return selection == null ? emptyList() : unmodifiableList(selection.getSelectionSpecs());
  }

  public Optional<FilteringExpression> getFilter() {
    return Optional.ofNullable(filter).map(Filter::getExpression);
  }

  public List<GroupingExpression> getAggregations() {
    return aggregation == null ? emptyList() : unmodifiableList(aggregation.getExpressions());
  }

  public Optional<FilteringExpression> getAggregationFilter() {
    return Optional.ofNullable(aggregationFilter).map(Filter::getExpression);
  }

  public List<SortingSpec> getSorts() {
    return sort == null ? emptyList() : unmodifiableList(sort.getSortingSpecs());
  }

  public Optional<Pagination> getPagination() {
    return Optional.ofNullable(pagination);
  }
}
