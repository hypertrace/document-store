package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.parser.FilteringExpressionParser;

/**
 * Expression representing a condition for filtering
 *
 * <p>Example: <code>
 * company IN ('Traceable', 'Harness')
 * </code> can be constructed as <code>
 * RelationalExpression.of( IdentifierExpression.of("company"), RelationalOperator.IN,
 * ConstantExpression.ofStrings("Traceable", "Harness"))));
 * </code>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RelationalExpression implements FilteringExpression {

  @NotNull SelectingExpression operand1;

  @NotNull RelationalOperator operator;

  @NotNull SelectingExpression operand2;

  public static RelationalExpression of(
      final SelectingExpression operand1,
      final RelationalOperator operator,
      final SelectingExpression operand2) {
    return validateAndReturn(new RelationalExpression(operand1, operator, operand2));
  }

  @Override
  public Object parse(final FilteringExpressionParser parser) {
    return parser.parse(this);
  }
}
