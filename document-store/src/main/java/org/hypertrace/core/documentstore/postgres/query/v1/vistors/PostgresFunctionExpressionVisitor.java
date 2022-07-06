package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

public class PostgresFunctionExpressionVisitor extends PostgresSelectTypeExpressionVisitor {
  @Override
  public String visit(final FunctionExpression expression) {
    int numArgs = expression.getOperands().size();
    if (numArgs == 0) {
      throw new IllegalArgumentException(
          String.format("%s should have at least one operand", expression));
    }

    SelectTypeExpressionVisitor selectTypeExpressionVisitor =
        new PostgresIdentifierExpressionVisitor(new PostgresConstantExpressionVisitor());

    if (numArgs == 1) {
      String value = expression.getOperands().get(0).accept(selectTypeExpressionVisitor);
      return String.format("%s( %s )", expression.getOperator().toString(), value);
    }

    Collector<String, ?, String> collector =
        getCollectorForFunctionOperator(expression.getOperator());

    String childList =
        expression.getOperands().stream()
            .map(exp -> exp.accept(selectTypeExpressionVisitor))
            .filter(str -> !StringUtils.isEmpty((String) str))
            .map(str -> "(" + str + ")")
            .collect(collector);

    return !childList.isEmpty() ? childList : null;
  }

  private Collector getCollectorForFunctionOperator(FunctionOperator operator) {
    if (operator.equals(FunctionOperator.ADD)) {
      return Collectors.joining(" + ");
    } else if (operator.equals(FunctionOperator.SUBTRACT)) {
      return Collectors.joining(" - ");
    } else if (operator.equals(FunctionOperator.MULTIPLY)) {
      return Collectors.joining(" * ");
    } else if (operator.equals(FunctionOperator.DIVIDE)) {
      return Collectors.joining(" / ");
    }
    throw new UnsupportedOperationException(
        String.format("Query operation:%s not supported", operator));
  }
}
