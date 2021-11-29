package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;
import org.hypertrace.core.documentstore.parser.GroupingExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortingExpressionVisitor;

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
    implements GroupingExpression, SelectingExpression, SortingExpression {

  @Singular @NotEmpty List<@NotNull SelectingExpression> operands;

  @NotNull FunctionOperator operator;

  public static class FunctionExpressionBuilder {
    public FunctionExpression build() {
      return validateAndReturn(new FunctionExpression(operands, operator));
    }
  }

  @Override
  public <T> T visit(final GroupingExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T visit(final SelectingExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T visit(final SortingExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
