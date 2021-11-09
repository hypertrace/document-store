package org.hypertrace.core.documentstore.query;

import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.FilteringExpression;
import org.hypertrace.core.documentstore.expression.GroupingExpression;
import org.hypertrace.core.documentstore.expression.SelectingExpression;

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
 *         .offset(5)
 *         .limit(10)
 *         .build();
 *  </code>
 */
@Value
@Builder
public class Query {
  List<Selection> selections;
  FilteringExpression filter;

  @Singular List<GroupingExpression> aggregations;
  FilteringExpression aggregationFilter;

  @Singular List<SortingDefinition> sortingDefinitions;

  int offset;
  int limit;

  public static class QueryBuilder {
    private List<Selection> selections;

    public QueryBuilder selection(SelectingExpression expression) {
      if (selections == null) {
        selections = new ArrayList<>();
      }

      selections.add(Selection.of(expression));
      return this;
    }

    public QueryBuilder selection(SelectingExpression expression, String alias) {
      if (selections == null) {
        selections = new ArrayList<>();
      }

      selections.add(Selection.of(expression, alias));
      return this;
    }
  }
}
