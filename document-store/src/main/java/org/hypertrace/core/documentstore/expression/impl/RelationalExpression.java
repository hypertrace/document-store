package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
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
public class RelationalExpression implements FilteringExpression {

  SelectingExpression lhs;

  RelationalOperator operator;

  SelectingExpression rhs;

  public static RelationalExpression of(
      final SelectingExpression lhs,
      final RelationalOperator operator,
      final SelectingExpression rhs) {
    Preconditions.checkArgument(lhs != null, "lhs is null");
    Preconditions.checkArgument(operator != null, "operator is null");
    Preconditions.checkArgument(rhs != null, "rhs is null");
    return new RelationalExpression(lhs, operator, rhs);
  }

  @Override
  public <T> T visit(final FilteringExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
