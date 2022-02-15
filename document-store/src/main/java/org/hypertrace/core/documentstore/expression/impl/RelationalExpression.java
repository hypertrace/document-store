package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterableExpression;
import org.hypertrace.core.documentstore.expression.type.SelectableExpression;
import org.hypertrace.core.documentstore.parser.FilterableExpressionVisitor;

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
public class RelationalExpression implements FilterableExpression {

  SelectableExpression lhs;

  RelationalOperator operator;

  SelectableExpression rhs;

  public static RelationalExpression of(
      final SelectableExpression lhs,
      final RelationalOperator operator,
      final SelectableExpression rhs) {
    Preconditions.checkArgument(lhs != null, "lhs is null");
    Preconditions.checkArgument(operator != null, "operator is null");
    Preconditions.checkArgument(rhs != null, "rhs is null");
    return new RelationalExpression(lhs, operator, rhs);
  }

  @Override
  public <T> T accept(final FilterableExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
