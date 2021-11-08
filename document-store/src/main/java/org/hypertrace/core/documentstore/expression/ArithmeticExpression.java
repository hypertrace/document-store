package org.hypertrace.core.documentstore.expression;

import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * Expression representing arithmetic operations in a query.
 *
 * Example:
 *   A-5 can be constructed as
 *   ArithmeticExpression.builder()
 *      .operand(LiteralExpression.of("A"))
 *      .operator(ArithmeticOperator.SUBTRACT)
 *      .operand(ConstantExpression.of(5))
 *      .build();
 *
 * The same can be constructed with different order of operands,
 * as long as the minuend is specified before the subtrahend
 * E.g.: Another valid ordering could be
 *
 * ArithmeticExpression.builder()
 *     .operator(ArithmeticOperator.SUBTRACT)
 *     .operand(LiteralExpression.of("A"))
 *     .operand(ConstantExpression.of(5))
 *     .build();
 */
@Value
@Builder
public class ArithmeticExpression implements Groupable, Projectable, Sortable {
  @Singular List<Projectable> operands;
  ArithmeticOperator operator;
}
