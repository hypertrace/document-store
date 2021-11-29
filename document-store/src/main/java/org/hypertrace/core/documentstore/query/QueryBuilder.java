package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.List;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;

@NoArgsConstructor
public class QueryBuilder {
  private Selection.SelectionBuilder selectionBuilder;
  private Filter.FilterBuilder filterBuilder;

  private Aggregation.AggregationBuilder aggregationBuilder;
  private Filter.FilterBuilder aggregationFilterBuilder;

  private Sort.SortBuilder sortBuilder;
  private Pagination pagination;

  public QueryBuilder setSelection(final Selection selection) {
    this.selectionBuilder = selection.toBuilder();
    return this;
  }

  public QueryBuilder addSelection(final SelectionSpec spec) {
    getSelectionBuilder().selectionSpec(spec);
    return this;
  }

  public QueryBuilder addSelection(final SelectingExpression expression) {
    addSelection(SelectionSpec.of(expression));
    return this;
  }

  public QueryBuilder addSelection(final SelectingExpression expression, final String alias) {
    addSelection(SelectionSpec.of(expression, alias));
    return this;
  }

  public QueryBuilder addSelections(final List<SelectionSpec> selectionSpecs) {
    if (CollectionUtils.isNotEmpty(selectionSpecs)) {
      getSelectionBuilder().selectionSpecs(selectionSpecs);
    }
    return this;
  }

  public QueryBuilder setFilter(final Filter filter) {
    this.filterBuilder = filter.toBuilder();
    return this;
  }

  public QueryBuilder setFilter(final FilteringExpression expression) {
    getFilterBuilder().expression(expression);
    return this;
  }

  public QueryBuilder setAggregation(final Aggregation aggregation) {
    this.aggregationBuilder = aggregation.toBuilder();
    return this;
  }

  public QueryBuilder addAggregation(final GroupingExpression expression) {
    getAggregationBuilder().expression(expression);
    return this;
  }

  public QueryBuilder addAggregations(final List<GroupingExpression> expressions) {
    if (CollectionUtils.isNotEmpty(expressions)) {
      getAggregationBuilder().expressions(expressions);
    }
    return this;
  }

  public QueryBuilder setAggregationFilter(final Filter filter) {
    this.aggregationFilterBuilder = filter.toBuilder();
    return this;
  }

  public QueryBuilder setAggregationFilter(final FilteringExpression expression) {
    getAggregationFilterBuilder().expression(expression);
    return this;
  }

  public QueryBuilder setSort(final Sort sort) {
    this.sortBuilder = sort.toBuilder();
    return this;
  }

  public QueryBuilder addSort(final SortingSpec spec) {
    getSortBuilder().sortingSpec(spec);
    return this;
  }

  public QueryBuilder addSort(final SortingExpression expression, final SortingOrder order) {
    addSort(SortingSpec.of(expression, order));
    return this;
  }

  public QueryBuilder addSorts(final List<SortingSpec> specs) {
    if (CollectionUtils.isNotEmpty(specs)) {
      getSortBuilder().sortingSpecs(specs);
    }
    return this;
  }

  public QueryBuilder setPagination(final Pagination pagination) {
    this.pagination = pagination;
    return this;
  }

  public Query build() {
    return validateAndReturn(
        new QueryInternal(
            getSelection(),
            getFilter(),
            getAggregation(),
            getAggregationFilter(),
            getSort(),
            pagination));
  }

  protected Selection.SelectionBuilder getSelectionBuilder() {
    return selectionBuilder == null ? selectionBuilder = Selection.builder() : selectionBuilder;
  }

  protected Filter.FilterBuilder getFilterBuilder() {
    return filterBuilder == null ? filterBuilder = Filter.builder() : filterBuilder;
  }

  protected Aggregation.AggregationBuilder getAggregationBuilder() {
    return aggregationBuilder == null
        ? aggregationBuilder = Aggregation.builder()
        : aggregationBuilder;
  }

  protected Filter.FilterBuilder getAggregationFilterBuilder() {
    return aggregationFilterBuilder == null
        ? aggregationFilterBuilder = Filter.builder()
        : aggregationFilterBuilder;
  }

  protected Sort.SortBuilder getSortBuilder() {
    return sortBuilder == null ? sortBuilder = Sort.builder() : sortBuilder;
  }

  private Selection getSelection() {
    return selectionBuilder == null ? null : selectionBuilder.build();
  }

  private Filter getFilter() {
    return filterBuilder == null ? null : filterBuilder.build();
  }

  private Aggregation getAggregation() {
    return aggregationBuilder == null ? null : aggregationBuilder.build();
  }

  private Filter getAggregationFilter() {
    return aggregationFilterBuilder == null ? null : aggregationFilterBuilder.build();
  }

  private Sort getSort() {
    return sortBuilder == null ? null : sortBuilder.build();
  }
}
