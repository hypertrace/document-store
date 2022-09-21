package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Set;
import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

@NoArgsConstructor
public class PostgresAggregateExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  private PostgresSelectTypeExpressionVisitor stringTypeDataAccessorVisitor;
  private PostgresSelectTypeExpressionVisitor numericTypeDataAccessorVisitor;

  private static Set<AggregationOperator> COUNT_OPERATOR =
      Set.of(AggregationOperator.COUNT, AggregationOperator.DISTINCT_COUNT);

  public PostgresAggregateExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
    intiSelectTypeExpressionVisitor();
  }

  public PostgresAggregateExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
    intiSelectTypeExpressionVisitor();
  }

  private void intiSelectTypeExpressionVisitor() {
    stringTypeDataAccessorVisitor =
        new PostgresFunctionExpressionVisitor(
            new PostgresDataAccessorIdentifierExpressionVisitor(
                new PostgresConstantExpressionVisitor(this), Type.STRING));

    numericTypeDataAccessorVisitor =
        new PostgresFunctionExpressionVisitor(
            new PostgresDataAccessorIdentifierExpressionVisitor(
                new PostgresConstantExpressionVisitor(this), Type.NUMERIC));
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @Override
  public String visit(final AggregateExpression expression) {
    AggregationOperator operator = expression.getAggregator();

    PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor =
        COUNT_OPERATOR.contains(expression.getAggregator())
            ? stringTypeDataAccessorVisitor
            : numericTypeDataAccessorVisitor;

    String value = expression.getExpression().accept(selectTypeExpressionVisitor).toString();
    return value != null ? convertToAggregationFunction(operator, value) : null;
  }

  private String convertToAggregationFunction(AggregationOperator operator, String value) {
    if (operator.equals(AggregationOperator.DISTINCT_COUNT)) {
      return String.format("COUNT(DISTINCT %s )", value);
    } else if (operator.equals(AggregationOperator.DISTINCT)) {
      return String.format("ARRAY_AGG(DISTINCT %s)", value);
    } else if (operator.equals(AggregationOperator.DISTINCT_ARRAY)) {
      return String.format("ARRAY_AGG(DISTINCT %s)", value);
    }
    return String.format("%s( %s )", operator, value);
  }
}
