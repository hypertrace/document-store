package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@NoArgsConstructor
public class PostgresFunctionExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  private static final int ARRAY_DIMENSION = 1;
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
      if (expression.getOperator().equals(FunctionOperator.LENGTH)) {
        return buildLengthExpression(expression.getOperands().get(0));
      }
      String parsedExpression = getParsedExpression(expression.getOperands().get(0));
      return String.format("%s( %s )", expression.getOperator(), parsedExpression);
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

  private String buildLengthExpression(final SelectTypeExpression operand) {
    Optional<String> identifier = Optional.ofNullable(operand.accept(identifierExpressionVisitor));
    Optional<String> resolvedSelection =
        identifier.map(v -> getPostgresQueryParser().getPgSelections().get(v));
    if (resolvedSelection.isPresent()) {
      return String.format(
          "COALESCE( ARRAY_LENGTH( %s, %s ), 0 )", resolvedSelection.get(), ARRAY_DIMENSION);
    }
    if (operand instanceof IdentifierExpression) {
      PostgresQueryParser parser = getPostgresQueryParser();
      return parser
          .getPgColTransformer()
          .buildArrayLengthExpression(parser.transformField((IdentifierExpression) operand));
    }
    PostgresFieldIdentifierExpressionVisitor fieldVisitor =
        new PostgresFieldIdentifierExpressionVisitor(getPostgresQueryParser());
    String parsedExpression = operand.accept(fieldVisitor);
    return String.format(
        "jsonb_array_length( CASE WHEN jsonb_typeof( %s ) = 'array' THEN %s"
            + " ELSE '[]'::jsonb END )",
        parsedExpression, parsedExpression);
  }

  private String getParsedExpression(final SelectTypeExpression expression) {
    Optional<String> identifier =
        Optional.ofNullable(expression.accept(identifierExpressionVisitor));
    return identifier
        .map(v -> getPostgresQueryParser().getPgSelections().get(v))
        .orElse(expression.accept(selectTypeExpressionVisitor));
  }
}
