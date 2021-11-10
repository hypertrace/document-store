package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.parser.FilteringExpressionParser;

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
public class LogicalExpression implements FilteringExpression {

  @Singular
  @Size(min = 2)
  @NotNull
  List<@NotNull FilteringExpression> operands;

  @NotNull LogicalOperator operator;

  public static class LogicalExpressionBuilder {
    public LogicalExpression build() {
      return validateAndReturn(new LogicalExpression(operands, operator));
    }
  }

  @Override
  public Object parse(FilteringExpressionParser parser) {
    return parser.parse(this);
  }
}
