package org.hypertrace.core.documentstore.expression.impl;

import static java.util.stream.Collectors.joining;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SortTypeExpression;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;

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
    implements GroupTypeExpression, SelectTypeExpression, SortTypeExpression {

  @Singular List<SelectTypeExpression> operands;

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
  public <T> T accept(final GroupTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SortTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return operator + "(" + operands.stream().map(String::valueOf).collect(joining(", ")) + ")";
  }
}
