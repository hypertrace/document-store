package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.CacheStrategy;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.parser.FilteringExpressionVisitor;

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
@EqualsAndHashCode(cacheStrategy = CacheStrategy.LAZY)
public class RelationalExpression implements FilteringExpression {

  @NotNull SelectingExpression lhs;

  @NotNull RelationalOperator operator;

  @NotNull SelectingExpression rhs;

  public static RelationalExpression of(
      final SelectingExpression lhs,
      final RelationalOperator operator,
      final SelectingExpression rhs) {
    return validateAndReturn(new RelationalExpression(lhs, operator, rhs));
  }

  @Override
  public <T> T visit(final FilteringExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
