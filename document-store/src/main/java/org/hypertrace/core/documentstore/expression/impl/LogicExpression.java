package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.LogicOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

/**
 * Expression to connect 2 or more relational expressions.
 *
 * <p>Example: <code>
 *     percentage >= 90 AND (college = 'IIT' OR college = 'NIT')
 *  </code> can be constructed as <code>
 *    LogicalExpression.of(
 *      RelationalExpression.of(
 *          IdentifierExpression.of("percentage"),
 *          RelationalOperator.GTE,
 *          ConstantExpression.of(90)),
 *      LogicalOperator.AND,
 *      LogicalExpression.of(
 *          RelationalExpression.of(
 *              IdentifierExpression.of("college"),
 *              RelationalOperator.EQ,
 *              ConstantExpression.of("IIT")),
 *          LogicalOperator.OR,
 *          RelationalExpression.of(
 *              IdentifierExpression.of("college"),
 *              RelationalOperator.EQ,
 *              ConstantExpression.of("NIT"))));
 *  </code>
 */
@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class LogicExpression implements FilterTypeExpression {

  @Singular List<FilterTypeExpression> operands;

  LogicOperator operator;

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  public static class LogicExpressionBuilder {
    public LogicExpression build() {
      Preconditions.checkArgument(operands != null, "operands is null");
      Preconditions.checkArgument(operands.size() >= 2, "At least 2 operands required");
      Preconditions.checkArgument(
          operands.stream().noneMatch(Objects::isNull), "One or more operands is null");
      Preconditions.checkArgument(operator != null, "operator is null");
      return new LogicExpression(operands, operator);
    }
  }
}
