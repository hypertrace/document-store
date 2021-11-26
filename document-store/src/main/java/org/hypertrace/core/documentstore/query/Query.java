package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.validation.Valid;
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
 *         .selection(IdentifierExpression.of("col4"))
 *         .selection(
 *             AggregateExpression.of(SUM, IdentifierExpression.of("col5")),
 *             "total")
 *         .filter(LogicalExpression.of(
 *             RelationalExpression.of(
 *                 IdentifierExpression.of("col1"),
 *                 LT,
 *                 ConstantExpression.of(7)),
 *             AND,
 *             RelationalExpression.of(
 *             IdentifierExpression.of("col2"),
 *             NEQ,
 *             IdentifierExpression.of("col3"))))
 *         .aggregation(IdentifierExpression.of("col4"))
 *         .aggregation(IdentifierExpression.of("col6"))
 *         .aggregationFilter(
 *             RelationalExpression.of(
 *                 AggregateExpression.of(SUM, IdentifierExpression.of("col5")),
 *                 GTE,
 *                 ConstantExpression.of(100)))
 *         .sort(
 *             FunctionExpression.builder()
 *                 .operand(IdentifierExpression.of("col7"))
 *                 .operator(ADD)
 *                 .operand(IdentifierExpression.of("col8"))
 *                 .build(),
 *             DESC)
 *         .offset(5)
 *         .limit(10)
 *         .build();
 *  </code>
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class Query {
  @Valid private final Selection selection; // Missing selection represents fetching all the columns
  @Valid private final Filter filter;

  @Valid private final Aggregation aggregation;
  @Valid private final Filter aggregationFilter;

  @Valid private final Sort sort;

  @Valid
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

    public QueryBuilder selection(final SelectingExpression expression) {
      getSelectionBuilder().selectionSpec(SelectionSpec.of(expression));
      return this;
    }

    public QueryBuilder selection(final SelectingExpression expression, final String alias) {
      getSelectionBuilder().selectionSpec(SelectionSpec.of(expression, alias));
      return this;
    }

    public QueryBuilder selections(final List<SelectionSpec> selectionSpecs) {
      getSelectionBuilder().selectionSpecs(selectionSpecs);
      return this;
    }

    public QueryBuilder filter(final FilteringExpression expression) {
      getFilterBuilder().expression(expression);
      return this;
    }

    public QueryBuilder filters(final List<FilteringExpression> expressions) {
      getFilterBuilder().filters(expressions);
      return this;
    }

    public QueryBuilder aggregation(final GroupingExpression expression) {
      getAggregationBuilder().expression(expression);
      return this;
    }

    public QueryBuilder aggregations(final List<GroupingExpression> expressions) {
      getAggregationBuilder().expressions(expressions);
      return this;
    }

    public QueryBuilder aggregationFilter(final FilteringExpression expression) {
      getAggregationFilterBuilder().expression(expression);
      return this;
    }

    public QueryBuilder aggregationFilters(final List<FilteringExpression> expressions) {
      getAggregationFilterBuilder().filters(expressions);
      return this;
    }

    public QueryBuilder sort(final SortingExpression expression, final SortingOrder order) {
      getSortBuilder().sortingSpec(SortingSpec.of(expression, order));
      return this;
    }

    public QueryBuilder sorts(final List<SortingSpec> specs) {
      getSortBuilder().sortingSpecs(specs);
      return this;
    }

    public QueryBuilder limit(final int limit) {
      getPaginationBuilder().limit(limit);
      return this;
    }

    public QueryBuilder offset(final int offset) {
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
