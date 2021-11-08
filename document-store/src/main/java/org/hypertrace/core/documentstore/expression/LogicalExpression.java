package org.hypertrace.core.documentstore.expression;

import lombok.Value;

/**
 * Expression to connect 2 or more relational expressions.
 *
 * <p>Example: <code>
 *     percentage >= 90 AND (college = 'IIT' OR college = 'NIT')
 *  </code> can be constructed as <code>
 *    LogicalExpression.of(
 *      RelationalExpression.of(
 *          LiteralExpression.of("percentage"),
 *          RelationalOperator.GTE,
 *          ConstantExpression.of(90)),
 *      LogicalOperator.AND,
 *      LogicalExpression.of(
 *          RelationalExpression.of(
 *              LiteralExpression.of("college"),
 *              RelationalOperator.EQ,
 *              ConstantExpression.of("IIT")),
 *          LogicalOperator.OR,
 *          RelationalExpression.of(
 *              LiteralExpression.of("college"),
 *              RelationalOperator.EQ,
 *              ConstantExpression.of("NIT"))));
 *  </code>
 */
@Value(staticConstructor = "of")
public class LogicalExpression implements Filterable {
  Filterable expression1;
  LogicalOperator operator;
  Filterable expression2;
}
