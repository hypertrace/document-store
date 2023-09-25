package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

/**
 * Expression representing a condition for filtering on array fields
 *
 * <p>Example: If product is an array field (containing objects)<code>
 * ANY(product).color IN ('Blue', 'Green')
 * </code> can be constructed as <code>
 *   ArrayFilterExpression.builder()
 *    .arrayOperator(ANY)
 *    .arraySource(IdentifierExpression.of("product"))
 *    .lhs(IdentifierExpression.of("color"))
 *    .operator(IN)
 *    .rhs(ConstantExpression.ofStrings("Blue", "Green"))
 *    .build();
 * </code>
 *
 * Note: The lhs is optional and can be dropped for primitive arrays
 *
 * <p>Example: If color is an array field <code>
 * ANY(color) IN ('Blue', 'Green')
 * </code> can be constructed as <code>
 *   ArrayFilterExpression.builder()
 *    .arrayOperator(ANY)
 *    .arraySource(IdentifierExpression.of("color"))
 *    .operator(IN)
 *    .rhs(ConstantExpression.ofStrings("Blue", "Green"))
 *    .build();
 * </code>
 */
@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ArrayFilterExpression implements FilterTypeExpression {
  ArrayOperator arrayOperator;

  SelectTypeExpression arraySource;

  @Nullable SelectTypeExpression lhs;

  RelationalOperator operator;

  SelectTypeExpression rhs;


  @SuppressWarnings("unused")
  public static class ArrayFilterExpressionBuilder {
    public ArrayFilterExpression build() {
      Preconditions.checkArgument(arrayOperator != null, "array operator is null");
      Preconditions.checkArgument(arraySource != null, "array source is null");
      Preconditions.checkArgument(operator != null, "operator is null");
      Preconditions.checkArgument(rhs != null, "rhs is null");
      return new ArrayFilterExpression(arrayOperator, arraySource, lhs, operator, rhs);
    }
  }

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return lhs == null ? String.format("%s(%s) %s %s", arrayOperator, arraySource, operator, rhs) : String.format(
        "%s(%s).%s %s %s", arrayOperator, arraySource, lhs, operator, rhs);
  }
}
