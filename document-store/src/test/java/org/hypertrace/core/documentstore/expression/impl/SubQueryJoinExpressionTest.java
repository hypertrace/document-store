package org.hypertrace.core.documentstore.expression.impl;

import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

class SubQueryJoinExpressionTest {

  /*
This is the query we want to execute:
SELECT suite_id, vulnerability_count, scan_run_number
FROM <implicit_collection>
JOIN (
    SELECT suite_id, MAX(scan_run_number) AS latest_scan_run_number
    FROM <implicit_collection>
    GROUP BY suite_id
) latest
ON suite_id = latest.suite_id
AND scan_run_number = latest.latest_scan_run_number;
   */

  void exampleUsage() {
    /*
    The right subquery:
    SELECT suite_id, MAX(scan_run_number) AS latest_scan_run_number
    FROM <implicit_collection>
    GROUP BY suite_id
    */
    Query subQuery = Query.builder()
        .addSelection(
            SelectionSpec.of(IdentifierExpression.of("suite_id"), "suite_id")
        )
        .addSelection(
            SelectionSpec.of(
                AggregateExpression.of(AggregationOperator.MAX, IdentifierExpression.of("scan_run_number")),
                "latest_scan_run_number"
        ))
        .addAggregation(
            IdentifierExpression.of("suite_id")
        )
        .build();
    
    /*
    The FROM expression representing a join with the right subquery:
    FROM <implicit_collection>
    JOIN (
       SELECT suite_id, MAX(scan_run_number) AS latest_scan_run_number
       FROM <implicit_collection>
       GROUP BY suite_id
    ) latest
    ON suite_id = latest.suite_id
    AND scan_run_number = latest.latest_scan_run_number;
    */
    SubQueryJoinExpression subQueryJoinExpression = SubQueryJoinExpression.builder()
        .subQuery(subQuery)
        .subQueryAlias("latest")
        .joinCondition(
            LogicalExpression.and(
                RelationalExpression.of(
                    IdentifierExpression.of("suite_id"),
                    RelationalOperator.EQ,
                    IdentifierExpression.of("latest.suite_id")
                ),
                RelationalExpression.of(
                    IdentifierExpression.of("scan_run_number"),
                    RelationalOperator.EQ,
                    IdentifierExpression.of("latest.latest_scan_run_number")
                )
        ))
        .build();

    /*
    Now build the top-level Query:
    SELECT suite_id, vulnerability_count, scan_run_number FROM <subQueryJoinExpression>
    */
    Query mainQuery = Query.builder()
        .addSelection(IdentifierExpression.of("suite_id"))
        .addSelection(IdentifierExpression.of("vulnerability_count"))
        .addSelection(IdentifierExpression.of("scan_run_number"))
        .addFromClause(subQueryJoinExpression)
        .build();

    System.out.println(mainQuery);

  }
}