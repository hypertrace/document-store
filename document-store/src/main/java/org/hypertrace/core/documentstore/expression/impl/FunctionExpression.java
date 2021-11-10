package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;
import org.hypertrace.core.documentstore.parser.GroupingExpressionParser;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;
import org.hypertrace.core.documentstore.parser.SortingExpressionParser;

/**
 * Expression representing arithmetic operations in a query.
 *
 * <p>Example: A-5 can be constructed as <code>
 *   ArithmeticExpression.builder()
 *      .operand(LiteralExpression.of("A"))
 *      .operator(ArithmeticOperator.SUBTRACT)
 *      .operand(ConstantExpression.of(5))
 *      .build();
 * </code> The same can be constructed with different order of operands, as long as the minuend is
 * specified before the subtrahend E.g.: Another valid ordering could be <code>
 *   ArithmeticExpression.builder()
 *     .operator(ArithmeticOperator.SUBTRACT)
 *     .operand(LiteralExpression.of("A"))
 *     .operand(ConstantExpression.of(5))
 *     .build();
 * </code>
 */
@Value
@Builder
public class FunctionExpression
    implements GroupingExpression, SelectingExpression, SortingExpression {

  @Singular
  @NotNull
  @Size(min = 1)
  List<@NotNull SelectingExpression> operands;

  @NotNull FunctionOperator operator;

  public static class FunctionExpressionBuilder {

    public FunctionExpression build() {
      return validateAndReturn(new FunctionExpression(operands, operator));
    }
  }

  @Override
  public Object parse(GroupingExpressionParser parser) {
    return parser.parse(this);
  }

  @Override
  public Object parse(SelectingExpressionParser parser) {
    return parser.parse(this);
  }

  @Override
  public Object parse(SortingExpressionParser parser) {
    return parser.parse(this);
  }
}
