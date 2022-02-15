package org.hypertrace.core.documentstore.query;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SortTypeExpression;

/**
 * A generic query definition that supports expressions. Note that this class is a more general
 * version of {@link org.hypertrace.core.documentstore.Query}
 *
 * <p>Example: <code>
 *     SELECT col4, SUM(col5) AS total
 *     FROM <collection>
 *     WHERE col1 < 7 AND col2 != col3
 *     GROUP BY col4, col6
 *     HAVING SUM(col5) >= 100
 *     ORDER BY col7+col8 DESC
 *     OFFSET 5
 *     LIMIT 10
 * </code> can be built as <code>
 *     Query query = Query.builder()
 *         .addSelection(IdentifierExpression.of("col4"))
 *         .addSelection(
 *             AggregateExpression.of(SUM, IdentifierExpression.of("col5")),
 *             "total")
 *         .setFilter(LogicalExpression.of(
 *             RelationalExpression.of(
 *                 IdentifierExpression.of("col1"),
 *                 LT,
 *                 ConstantExpression.of(7)),
 *             AND,
 *             RelationalExpression.of(
 *                  IdentifierExpression.of("col2"),
 *                  NEQ,
 *                  IdentifierExpression.of("col3"))))
 *         .addAggregation(IdentifierExpression.of("col4"))
 *         .addAggregation(IdentifierExpression.of("col6"))
 *         .setAggregationFilter(
 *             RelationalExpression.of(
 *                 AggregateExpression.of(SUM, IdentifierExpression.of("col5")),
 *                 GTE,
 *                 ConstantExpression.of(100)))
 *         .addSort(
 *             FunctionExpression.builder()
 *                 .operand(IdentifierExpression.of("col7"))
 *                 .operator(ADD)
 *                 .operand(IdentifierExpression.of("col8"))
 *                 .build(),
 *             DESC)
 *         .setOffset(5)
 *         .setLimit(10)
 *         .build();
 *  </code>
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@ToString
public final class Query {
  private final Selection selection; // Missing selection represents fetching all the columns
  private final Filter filter;

  private final Aggregation aggregation;
  private final Filter aggregationFilter;

  private final Sort sort;
  private final Pagination pagination; // Missing pagination represents fetching all the records

  public List<SelectionSpec> getSelections() {
    return selection == null ? emptyList() : unmodifiableList(selection.getSelectionSpecs());
  }

  public Optional<FilterTypeExpression> getFilter() {
    return Optional.ofNullable(filter).map(Filter::getExpression);
  }

  public List<GroupTypeExpression> getAggregations() {
    return aggregation == null ? emptyList() : unmodifiableList(aggregation.getExpressions());
  }

  public Optional<FilterTypeExpression> getAggregationFilter() {
    return Optional.ofNullable(aggregationFilter).map(Filter::getExpression);
  }

  public List<SortingSpec> getSorts() {
    return sort == null ? emptyList() : unmodifiableList(sort.getSortingSpecs());
  }

  public Optional<Pagination> getPagination() {
    return Optional.ofNullable(pagination);
  }

  public static QueryBuilder builder() {
    return new QueryBuilder();
  }

  @NoArgsConstructor
  public static class QueryBuilder {
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

    public QueryBuilder setSelections(final List<SelectionSpec> selectionSpecs) {
      if (CollectionUtils.isNotEmpty(selectionSpecs)) {
        getSelectionBuilder().clearSelectionSpecs();
      }

      return addSelections(selectionSpecs);
    }

    public QueryBuilder addSelection(final SelectionSpec spec) {
      getSelectionBuilder().selectionSpec(spec);
      return this;
    }

    public QueryBuilder addSelection(final SelectTypeExpression expression) {
      addSelection(SelectionSpec.of(expression));
      return this;
    }

    public QueryBuilder addSelection(final SelectTypeExpression expression, final String alias) {
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

    public QueryBuilder setFilter(final FilterTypeExpression expression) {
      getFilterBuilder().expression(expression);
      return this;
    }

    public QueryBuilder setAggregation(final Aggregation aggregation) {
      this.aggregationBuilder = aggregation.toBuilder();
      return this;
    }

    public QueryBuilder setAggregations(final List<GroupTypeExpression> expressions) {
      if (CollectionUtils.isNotEmpty(expressions)) {
        getAggregationBuilder().clearExpressions();
      }
      return addAggregations(expressions);
    }

    public QueryBuilder addAggregation(final GroupTypeExpression expression) {
      getAggregationBuilder().expression(expression);
      return this;
    }

    public QueryBuilder addAggregations(final List<GroupTypeExpression> expressions) {
      if (CollectionUtils.isNotEmpty(expressions)) {
        getAggregationBuilder().expressions(expressions);
      }
      return this;
    }

    public QueryBuilder setAggregationFilter(final Filter filter) {
      this.aggregationFilterBuilder = filter.toBuilder();
      return this;
    }

    public QueryBuilder setAggregationFilter(final FilterTypeExpression expression) {
      getAggregationFilterBuilder().expression(expression);
      return this;
    }

    public QueryBuilder setSort(final Sort sort) {
      this.sortBuilder = sort.toBuilder();
      return this;
    }

    public QueryBuilder setSorts(final List<SortingSpec> specs) {
      if (CollectionUtils.isNotEmpty(specs)) {
        getSortBuilder().clearSortingSpecs();
      }
      return addSorts(specs);
    }

    public QueryBuilder addSort(final SortingSpec spec) {
      getSortBuilder().sortingSpec(spec);
      return this;
    }

    public QueryBuilder addSort(final SortTypeExpression expression, final SortOrder order) {
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
      return new Query(
          getSelection(),
          getFilter(),
          getAggregation(),
          getAggregationFilter(),
          getSort(),
          pagination);
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
}
