package org.hypertrace.core.documentstore.expression.impl;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

/**
 * Expression representing a join operation.
 *
 * <p>Example: In the below query,</p>
 * <code>
 * SELECT sa.suite_id, sa.vulnerability_count, sa.scan_run_number
 * FROM scan_analytics sa
 * JOIN (
 *     SELECT suite_id, MAX(scan_run_number) AS latest_scan_run_number
 *     FROM scan_analytics
 *     GROUP BY suite_id
 * ) latest
 * ON sa.suite_id = latest.suite_id
 * AND sa.scan_run_number = latest.latest_scan_run_number;
 * </code>
 *
 * <p>The join expression is</p>
 * <code>
 * scan_analytics sa
 * JOIN (
 *     SELECT suite_id, MAX(scan_run_number) AS latest_scan_run_number
 *     FROM scan_analytics
 *     GROUP BY suite_id
 * ) latest
 * ON sa.suite_id = latest.suite_id
 * AND sa.scan_run_number = latest.latest_scan_run_number;
 * </code>
 *
 * <p>which can be constructed as </p>
 * <code>
 *       Query subQuery = Query.builder()
 *         .addSelection(
 *             SelectionSpec.of(IdentifierExpression.of("suite_id"), "suite_id")
 *         )
 *         .addSelection(
 *             SelectionSpec.of(
 *                 AggregateExpression.of(AggregationOperator.MAX, IdentifierExpression.of("scan_run_number")),
 *                 "latest_scan_run_number"
 *         ))
 *         .addFromClause(
 *             TableFromExpression.builder()
 *                 .alias("ignored")
 *                 .build()
 *         )
 *         .addAggregation(
 *             IdentifierExpression.of("suite_id")
 *         )
 *         .build();
 *
 * // The main FROM side: "scan_analytics sa"
 *     FromTypeExpression leftTable = TableFromExpression.builder()
 *         .alias("sa")
 *         .build();
 *
 * // The subquery side: "(...subQuery...) latest"
 *     FromTypeExpression rightSubQuery = SubQueryFromExpression.builder()
 *         .subQuery(subQuery)
 *         .alias("latest")
 *         .build();
 *
 * // The ON condition:
 * //   sa.suite_id = latest.suite_id
 * //   AND sa.scan_run_number = latest.latest_scan_run_number
 *     FilterTypeExpression onCondition = LogicalExpression.and(
 *             RelationalExpression.of(
 *                 IdentifierExpression.of("sa.suite_id"),
 *                 RelationalOperator.EQ,
 *                 IdentifierExpression.of("latest.suite_id")
 *                 ),
 *             RelationalExpression.of(
 *                 IdentifierExpression.of("a.scan_run_number"),
 *                 RelationalOperator.EQ,
 *                 IdentifierExpression.of("latest.latest_scan_run_number")
 *             )
 *     );
 *
 *     JoinExpression joinExpression = JoinExpression.builder()
 *         .left(leftTable)
 *         .right(rightSubQuery)
 *         .joinType(JoinType.INNER)
 *         .onCondition(onCondition)
 *         .build();
 * </code>
 */
@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JoinExpression implements FromTypeExpression {
  FromTypeExpression left;
  FromTypeExpression right;
  FilterTypeExpression onCondition;

  @Override
  public <T> T accept(FromTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}

