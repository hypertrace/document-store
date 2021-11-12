package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.collections4.CollectionUtils;
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
 *         .sortingDefinition(
 *             FunctionExpression.builder()
 *                 .operand(IdentifierExpression.of("col7"))
 *                 .operator(ADD)
 *                 .operand(IdentifierExpression.of("col8"))
 *                 .build(),
 *             DESC)
 *         .paginationDefinition(PaginationDefinition.of(10, 5))
 *         .build();
 *  </code>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class Query {

  @NotEmpty List<@NotNull Selection> selections;

  FilteringExpression filter;

  @Singular
  @Size(min = 1)
  List<@NotNull GroupingExpression> aggregations;

  FilteringExpression aggregationFilter;

  @Size(min = 1)
  List<@NotNull SortingDefinition> sortingDefinitions;

  // Missing pagination definition represents fetching all the records
  PaginationDefinition paginationDefinition;

  public static class QueryBuilder {

    public Query build() {
      return validateAndReturn(
          new Query(
              selections,
              filter,
              aggregations,
              aggregationFilter,
              sortingDefinitions,
              paginationDefinition));
    }

    public QueryBuilder selection(Selection selection) {
      addSelection(selection);
      return this;
    }

    public QueryBuilder selection(SelectingExpression expression) {
      addSelection(WhitelistedSelection.of(expression));
      return this;
    }

    public QueryBuilder selection(SelectingExpression expression, String alias) {
      addSelection(WhitelistedSelection.of(expression, alias));
      return this;
    }

    public QueryBuilder sortingDefinition(SortingExpression expression, SortingOrder order) {
      if (CollectionUtils.isEmpty(sortingDefinitions)) {
        sortingDefinitions = new ArrayList<>();
      }

      sortingDefinitions.add(SortingDefinition.of(expression, order));
      return this;
    }

    private void addSelection(Selection selection) {
      if (CollectionUtils.isEmpty(selections)) {
        selections = new ArrayList<>();
      }

      selections.add(selection);
    }
  }
}
