package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@NoArgsConstructor
public class PostgresFunctionExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresFunctionExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  public PostgresFunctionExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @Override
  public String visit(final FunctionExpression expression) {
    int numArgs = expression.getOperands().size();
    if (numArgs == 0) {
      throw new IllegalArgumentException(
          String.format("%s should have at least one operand", expression));
    }

    PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor =
        new PostgresDataAccessorIdentifierExpressionVisitor(
            new PostgresConstantExpressionVisitor(getPostgresQueryParser()));

    if (numArgs == 1) {
      String value = expression.getOperands().get(0).accept(selectTypeExpressionVisitor);
      return String.format("%s( %s )", expression.getOperator().toString(), value);
    }

    Collector<String, ?, String> collector =
        getCollectorForFunctionOperator(expression.getOperator());

    String childList =
        expression.getOperands().stream()
            .map(exp -> exp.accept(selectTypeExpressionVisitor))
            .filter(Objects::nonNull)
            .map(Object::toString)
            .filter(StringUtils::isNotEmpty)
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
