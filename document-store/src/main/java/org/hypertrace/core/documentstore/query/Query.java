package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;

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
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class Query {
  private final Selection selection; // Missing selection represents fetching all the columns
  private final Filter filter;

  private final Aggregation aggregation;
  private final Filter aggregationFilter;

  private final Sort sort;
  private final Pagination pagination; // Missing pagination represents fetching all the records

  public static QueryBuilder builder() {
    return new QueryBuilder();
  }

  public List<SelectionSpec> getSelections() {
    if (selection == null) {
      return Collections.emptyList();
    }

    return selection.getSelectionSpecs();
  }

  public Optional<FilteringExpression> getFilter() {
    return Optional.ofNullable(filter).map(Filter::getExpression);
  }

  public List<GroupingExpression> getAggregations() {
    if (aggregation == null) {
      return Collections.emptyList();
    }

    return aggregation.getExpressions();
  }

  public Optional<FilteringExpression> getAggregationFilter() {
    return Optional.ofNullable(aggregationFilter).map(Filter::getExpression);
  }

  public List<SortingSpec> getSorts() {
    if (sort == null) {
      return Collections.emptyList();
    }

    return sort.getSortingSpecs();
  }

  public Optional<Pagination> getPagination() {
    return Optional.ofNullable(pagination);
  }

  public static final class QueryBuilder {
    private Selection.SelectionBuilder selectionBuilder;
    private Filter.FilterBuilder filterBuilder;

    private Aggregation.AggregationBuilder aggregationBuilder;
    private Filter.FilterBuilder aggregationFilterBuilder;

    private Sort.SortBuilder sortBuilder;
    private Pagination.PaginationBuilder paginationBuilder;

    private QueryBuilder() {}

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
      getSelectionBuilder().selectionSpecs(selectionSpecs);
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
      getAggregationBuilder().expressions(expressions);
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
      getSortBuilder().sortingSpecs(specs);
      return this;
    }

    public QueryBuilder setPagination(final Pagination pagination) {
      this.paginationBuilder = pagination.toBuilder();
      return this;
    }

    public QueryBuilder setLimit(final int limit) {
      getPaginationBuilder().limit(limit);
      return this;
    }

    public QueryBuilder setOffset(final int offset) {
      getPaginationBuilder().offset(offset);
      return this;
    }

    public Query build() {
      return validateAndReturn(
          new Query(
              getSelection(),
              getFilter(),
              getAggregation(),
              getAggregationFilter(),
              getSort(),
              getPagination()));
    }

    private Selection.SelectionBuilder getSelectionBuilder() {
      return selectionBuilder == null ? selectionBuilder = Selection.builder() : selectionBuilder;
    }

    private Selection getSelection() {
      return selectionBuilder == null ? null : selectionBuilder.build();
    }

    private Filter.FilterBuilder getFilterBuilder() {
      return filterBuilder == null ? filterBuilder = Filter.builder() : filterBuilder;
    }

    private Filter getFilter() {
      return filterBuilder == null ? null : filterBuilder.build();
    }

    private Aggregation.AggregationBuilder getAggregationBuilder() {
      return aggregationBuilder == null
          ? aggregationBuilder = Aggregation.builder()
          : aggregationBuilder;
    }

    private Aggregation getAggregation() {
      return aggregationBuilder == null ? null : aggregationBuilder.build();
    }

    private Filter.FilterBuilder getAggregationFilterBuilder() {
      return aggregationFilterBuilder == null
          ? aggregationFilterBuilder = Filter.builder()
          : aggregationFilterBuilder;
    }

    private Filter getAggregationFilter() {
      return aggregationFilterBuilder == null ? null : aggregationFilterBuilder.build();
    }

    private Sort.SortBuilder getSortBuilder() {
      return sortBuilder == null ? sortBuilder = Sort.builder() : sortBuilder;
    }

    private Sort getSort() {
      return sortBuilder == null ? null : sortBuilder.build();
    }

    private Pagination.PaginationBuilder getPaginationBuilder() {
      return paginationBuilder == null
          ? paginationBuilder = Pagination.builder()
          : paginationBuilder;
    }

    private Pagination getPagination() {
      return paginationBuilder == null ? null : paginationBuilder.build();
    }
  }
}
