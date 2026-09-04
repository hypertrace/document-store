package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

/**
 * Expression representing a condition for filtering on array fields.
 *
 * <p>When the inner filter uses {@code IN}, that operator is <em>element membership</em>, not array
 * equality. Combined with {@link ArrayOperator}:
 *
 * <ul>
 *   <li>{@code ALL}: the RHS set is a subset of the stored array (every listed value appears in
 *       the array).
 *   <li>{@code EXACTLY_ONE}: the stored array has length 1 and that element is in the RHS set.
 * </ul>
 *
 * Order and duplicates are ignored (set semantics).
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
public class ArrayRelationalFilterExpression
    implements FilterTypeExpression, ArrayFilterExpression {
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
  public SelectTypeExpression getArraySource() {
    return filter.getLhs();
  }

  @Override
  public String toString() {
    return String.format(
        "%s(%s) %s %s", operator, filter.getLhs(), filter.getOperator(), filter.getRhs());
  }
}
