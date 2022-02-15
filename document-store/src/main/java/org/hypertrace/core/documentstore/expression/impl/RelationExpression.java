package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.RelationOperator;
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
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RelationExpression implements FilterTypeExpression {

  SelectTypeExpression lhs;

  RelationOperator operator;

  SelectTypeExpression rhs;

  public static RelationExpression of(
      final SelectTypeExpression lhs,
      final RelationOperator operator,
      final SelectTypeExpression rhs) {
    Preconditions.checkArgument(lhs != null, "lhs is null");
    Preconditions.checkArgument(operator != null, "operator is null");
    Preconditions.checkArgument(rhs != null, "rhs is null");
    return new RelationExpression(lhs, operator, rhs);
  }

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
