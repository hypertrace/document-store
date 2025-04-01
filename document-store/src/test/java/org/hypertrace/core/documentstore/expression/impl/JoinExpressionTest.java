package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.*;

import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.query.FromClause;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

class JoinExpressionTest {

  /*
This is the query we want to execute:
SELECT sa.suite_id, sa.vulnerability_count, sa.scan_run_number
FROM scan_analytics sa
JOIN (
    SELECT suite_id, MAX(scan_run_number) AS latest_scan_run_number
    FROM scan_analytics
    GROUP BY suite_id
) latest
ON sa.suite_id = latest.suite_id
AND sa.scan_run_number = latest.latest_scan_run_number;
   */

  void exampleUsage() {
    // The right subquery:
// SELECT suite_id, MAX(scan_run_number) AS latest_scan_run_number
// FROM scan_analytics
// GROUP BY suite_id
    Query subQuery = Query.builder()
        // SELECT
        .addSelection(
            SelectionSpec.of(IdentifierExpression.of("suite_id"), "suite_id")
        )
        .addSelection(
            SelectionSpec.of(
                AggregateExpression.of(AggregationOperator.MAX, IdentifierExpression.of("scan_run_number")),
                "latest_scan_run_number"
        ))
        // FROM
        .addFromClause(
            TableFromExpression.builder()
                .alias("ignored")  // or skip if not needed
                .build()
        )
        // GROUP BY
        .addAggregation(
            IdentifierExpression.of("suite_id")
        )
        .build();

// The main FROM side: "scan_analytics sa"
    FromTypeExpression leftTable = TableFromExpression.builder()
        .alias("sa")
        .build();

// The subquery side: "(...subQuery...) latest"
    FromTypeExpression rightSubQuery = SubQueryFromExpression.builder()
        .subQuery(subQuery)
        .alias("latest")
        .build();

// The ON condition:
//   sa.suite_id = latest.suite_id
//   AND sa.scan_run_number = latest.latest_scan_run_number
    FilterTypeExpression onCondition = LogicalExpression.and(
            RelationalExpression.of(
                IdentifierExpression.of("sa.suite_id"),
                RelationalOperator.EQ,
                IdentifierExpression.of("latest.suite_id")
                ),
            RelationalExpression.of(
                IdentifierExpression.of("a.scan_run_number"),
                RelationalOperator.EQ,
                IdentifierExpression.of("latest.latest_scan_run_number")
            )
    );

    JoinExpression joinExpression = JoinExpression.builder()
        .left(leftTable)
        .right(rightSubQuery)
        .joinType(JoinType.INNER)
        .onCondition(onCondition)
        .build();

// Now build the top-level Query:
// SELECT sa.suite_id, sa.vulnerability_count, sa.scan_run_number
    Query mainQuery = Query.builder()
        .addSelection(
            SelectionSpec.of(IdentifierExpression.of("sa.suite_id"), "suite_id")
        )
        .addSelection(
            SelectionSpec.of(IdentifierExpression.of("sa.vulnerability_count"), "vulnerability_count")
        )
        .addSelection(
            SelectionSpec.of(IdentifierExpression.of("sa.scan_run_number"), "scan_run_number")
        )
        .addFromClause(joinExpression)
        .build();

    System.out.println(mainQuery);

  }
}