package org.hypertrace.core.documentstore.query;

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
public abstract class Query {
  public static QueryBuilder builder() {
    return new QueryBuilder();
  }
}
