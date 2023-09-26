package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

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
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RelationalExpression implements FilterTypeExpression {

  @Default SelectTypeExpression lhs = RootExpression.INSTANCE;

  RelationalOperator operator;

  SelectTypeExpression rhs;

  public static RelationalExpression of(
      final SelectTypeExpression lhs,
      final RelationalOperator operator,
      final SelectTypeExpression rhs) {
    Preconditions.checkArgument(lhs != null, "lhs is null");
    Preconditions.checkArgument(operator != null, "operator is null");
    Preconditions.checkArgument(rhs != null, "rhs is null");
    return new RelationalExpression(lhs, operator, rhs);
  }

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return lhs + " " + operator + " " + rhs;
  }
}
