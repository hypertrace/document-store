package org.hypertrace.core.documentstore.expression;

import lombok.Value;

/**
 * Expression representing a condition for filtering
 *
 * <p>Example: <code>
 *     company IN ('Traceable', 'Harness')
 *  </code> can be constructed as <code>
 *     RelationalExpression.of(
 *         LiteralExpression.of("company"),
 *         RelationalOperator.IN,
 *         ConstantExpression.ofStrings("Traceable", "Harness"))));
 * </code>
 */
@Value(staticConstructor = "of")
public class RelationalExpression implements Filterable {
  Projectable operand1;
  RelationalOperator operator;
  Projectable operand2;
}
