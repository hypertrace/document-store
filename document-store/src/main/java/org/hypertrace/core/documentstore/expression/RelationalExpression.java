package org.hypertrace.core.documentstore.expression;

import lombok.Value;
import org.hypertrace.core.documentstore.parser.FilteringExpressionParser;

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
public class RelationalExpression implements FilteringExpression {
  SelectingExpression operand1;
  RelationalOperator operator;
  SelectingExpression operand2;

  @Override
  public Object parse(FilteringExpressionParser parser) {
    return parser.parse(this);
  }
}
