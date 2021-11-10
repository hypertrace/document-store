package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;

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
 *         .whereFilter(LogicalExpression.of(
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
 *         .havingFilter(
 *             RelationalExpression.of(
 *                 AggregateExpression.of(SUM, IdentifierExpression.of("col5")),
 *                 GTE,
 *                 ConstantExpression.of(100)))
 *         .orderBy(
 *             OrderBy.of(
 *                 FunctionExpression.builder()
 *                     .operand(IdentifierExpression.of("col7"))
 *                     .operator(ADD)
 *                     .operand(IdentifierExpression.of("col8")).build(),
 *                 DESC))
 *         .paginationDefinition(PaginationDefinition.of(5, 10))
 *         .build();
 *  </code>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class Query {

  @Size(min = 1)
  List<@NotNull Selection> selections;

  FilteringExpression filter;

  @Singular
  @Size(min = 1)
  List<@NotNull GroupingExpression> aggregations;

  FilteringExpression aggregationFilter;

  @Singular
  @Size(min = 1)
  List<@NotNull SortingDefinition> sortingDefinitions;

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
  }
}
