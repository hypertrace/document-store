package org.hypertrace.core.documentstore.expression.impl;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.NOT;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
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
public class LogicalExpression implements FilterTypeExpression {

  @Singular List<FilterTypeExpression> operands;

  LogicalOperator operator;

  public static LogicalExpression and(final Collection<FilterTypeExpression> operands) {
    return buildLogicalExpression(operands, AND);
  }

  public static LogicalExpression and(
      final FilterTypeExpression operand1,
      final FilterTypeExpression operand2,
      final FilterTypeExpression... otherOperands) {
    return buildLogicalExpression(operand1, operand2, otherOperands, AND);
  }

  public static LogicalExpression or(final Collection<FilterTypeExpression> operands) {
    return buildLogicalExpression(operands, OR);
  }

  public static LogicalExpression or(
      final FilterTypeExpression operand1,
      final FilterTypeExpression operand2,
      final FilterTypeExpression... otherOperands) {
    return buildLogicalExpression(operand1, operand2, otherOperands, OR);
  }

  public static LogicalExpression not(final FilterTypeExpression operand) {
    return buildLogicalExpression(List.of(operand), NOT);
  }

  private static LogicalExpression buildLogicalExpression(
      final FilterTypeExpression operand1,
      final FilterTypeExpression operand2,
      final FilterTypeExpression[] otherOperands,
      final LogicalOperator operator) {
    return LogicalExpression.builder()
        .operator(operator)
        .operand(operand1)
        .operand(operand2)
        .operands(Arrays.stream(otherOperands).collect(toUnmodifiableList()))
        .build();
  }

  private static LogicalExpression buildLogicalExpression(
      final Collection<FilterTypeExpression> operands, final LogicalOperator operator) {
    return LogicalExpression.builder()
        .operator(operator)
        .operands(operands.stream().collect(toUnmodifiableList()))
        .build();
  }

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    if (NOT.equals(operator)) {
      return String.format("NOT (%s)", operands.get(0));
    }

    return "("
        + operands.stream().map(String::valueOf).collect(joining(") " + operator + " ("))
        + ")";
  }

  public static class LogicalExpressionBuilder {
    public LogicalExpression build() {
      Preconditions.checkArgument(operator != null, "operator is null");
      Preconditions.checkArgument(operands != null, "operands is null");

      if (NOT.equals(operator)) {
        Preconditions.checkArgument(operands.size() == 1, "Exactly one operand required for NOT");
      } else {
        Preconditions.checkArgument(operands.size() >= 2, "At least 2 operands required");
      }

      Preconditions.checkArgument(
          operands.stream().noneMatch(Objects::isNull), "One or more operands is null");
      return new LogicalExpression(operands, operator);
    }
  }
}
