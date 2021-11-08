package org.hypertrace.core.documentstore.expression;

import lombok.Value;

/**
 * Expression representing a condition for filtering
 *
 * <p>Example: company IN ('Traceable', 'Harness')
 *
 * <p>can be constructed as
 *
 * <p>RelationalExpression.of( LiteralExpression.of("company"), RelationalOperator.IN,
 * ConstantExpression.ofStrings("Traceable", "Harness"))));
 */
@Value(staticConstructor = "of")
public class RelationalExpression implements Filterable {
  Projectable operand1;
  RelationalOperator operator;
  Projectable operand2;
}
