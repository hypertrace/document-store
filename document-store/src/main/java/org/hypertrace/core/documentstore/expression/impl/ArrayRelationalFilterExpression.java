package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

/**
 * Expression representing a condition for filtering on array fields
 *
 * <p>Example: If color is an array field <code>
 * ANY(color) IN ('Blue', 'Green')
 * </code> can be constructed as <code>
 *   ArrayRelationalFilterExpression.builder()
 *    .operator(ANY)
 *    .filter(
 *      RelationalExpression.of(
 *        IdentifierExpression.of("color"),
 *        IN,
 *        ConstantExpression.ofStrings("Blue", "Green")
 *    )
 *    .build();
 * </code>
 */
@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ArrayRelationalFilterExpression implements FilterTypeExpression {
  ArrayOperator operator;

  RelationalExpression filter;

  @SuppressWarnings("unused")
  public static class ArrayRelationalFilterExpressionBuilder {
    public ArrayRelationalFilterExpression build() {
      Preconditions.checkArgument(operator != null, "array operator is null");
      Preconditions.checkArgument(filter != null, "filter is null");
      return new ArrayRelationalFilterExpression(operator, filter);
    }
  }

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return String.format(
        "%s(%s) %s %s", operator, filter.getLhs(), filter.getOperator(), filter.getRhs());
  }
}
