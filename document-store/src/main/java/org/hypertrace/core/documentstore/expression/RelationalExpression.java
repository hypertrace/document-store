package org.hypertrace.core.documentstore.expression;

import lombok.Value;

/**
 *  Expression representing a condition for filtering
 *
 *  Example:
 *     company IN ('Traceable', 'Harness')
 *
 *  can be constructed as
 *
 *     RelationalExpression.of(
 *         LiteralExpression.of("company"),
 *         RelationalOperator.IN,
 *         ConstantExpression.ofStrings("Traceable", "Harness"))));
 */
@Value(staticConstructor = "of")
public class RelationalExpression implements Filterable {
  Projectable operand1;
  RelationalOperator operator;
  Projectable operand2;
}
