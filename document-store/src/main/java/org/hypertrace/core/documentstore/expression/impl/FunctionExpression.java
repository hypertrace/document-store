package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.expression.type.GroupableExpression;
import org.hypertrace.core.documentstore.expression.type.SelectableExpression;
import org.hypertrace.core.documentstore.expression.type.SortableExpression;
import org.hypertrace.core.documentstore.parser.GroupableExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectableExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortableExpressionVisitor;

/**
 * Expression representing arithmetic/function operations in a query.
 *
 * <p>Example: A-5 can be constructed as <code>
 *      FunctionExpression.builder()
 *        .operand(LiteralExpression.of("A"))
 *        .operator(FunctionOperator.SUBTRACT)
 *        .operand(ConstantExpression.of(5))
 *        .build();
 * </code> The same can be constructed with different order of operands, as long as the minuend is
 * specified before the subtrahend.
 *
 * <p>E.g.: Another valid ordering could be <code>
 *      FunctionExpression.builder()
 *        .operator(FunctionOperator.SUBTRACT)
 *        .operand(LiteralExpression.of("A"))
 *        .operand(ConstantExpression.of(5))
 *        .build();
 * </code>
 */
@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FunctionExpression
    implements GroupableExpression, SelectableExpression, SortableExpression {

  @Singular List<SelectableExpression> operands;

  FunctionOperator operator;

  public static class FunctionExpressionBuilder {
    public FunctionExpression build() {
      Preconditions.checkArgument(!operands.isEmpty(), "operands is empty");
      Preconditions.checkArgument(
          operands.stream().noneMatch(Objects::isNull), "One or more operands is null");
      Preconditions.checkArgument(operator != null, "operator is null");
      return new FunctionExpression(operands, operator);
    }
  }

  @Override
  public <T> T accept(final GroupableExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SelectableExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SortableExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
