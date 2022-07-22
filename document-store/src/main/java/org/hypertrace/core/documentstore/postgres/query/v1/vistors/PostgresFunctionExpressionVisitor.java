package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@NoArgsConstructor
public class PostgresFunctionExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  private PostgresIdentifierExpressionVisitor identifierExpressionVisitor;
  private PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor;

  public PostgresFunctionExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
    initExpressionVisitor();
  }

  public PostgresFunctionExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
    initExpressionVisitor();
  }

  private void initExpressionVisitor() {
    identifierExpressionVisitor = new PostgresIdentifierExpressionVisitor(getPostgresQueryParser());
    selectTypeExpressionVisitor =
        new PostgresDataAccessorIdentifierExpressionVisitor(
            new PostgresConstantExpressionVisitor(getPostgresQueryParser()));
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

    if (numArgs == 1) {
      String parsedExpression = getParsedExpression(expression.getOperands().get(0));
      return expression.getOperator().equals(FunctionOperator.LENGTH)
          ? String.format("ARRAY_LENGTH( %s, 1 )", parsedExpression)
          : String.format("%s( %s )", expression.getOperator(), parsedExpression);
    }

    Collector<String, ?, String> collector =
        getCollectorForFunctionOperator(expression.getOperator());

    String childList =
        expression.getOperands().stream()
            .map(exp -> getParsedExpression(exp))
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

  private String getParsedExpression(final SelectTypeExpression expression) {
    Optional<String> identifier =
        Optional.ofNullable(expression.accept(identifierExpressionVisitor));
    return identifier
        .map(v -> getPostgresQueryParser().getPgSelections().get(v))
        .orElse(expression.accept(selectTypeExpressionVisitor));
  }
}
