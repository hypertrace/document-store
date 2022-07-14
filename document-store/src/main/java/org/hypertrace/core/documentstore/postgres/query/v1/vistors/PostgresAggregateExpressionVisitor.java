package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@NoArgsConstructor
public class PostgresAggregateExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresAggregateExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  public PostgresAggregateExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @Override
  public String visit(final AggregateExpression expression) {
    AggregationOperator operator = expression.getAggregator();
    PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor =
        new PostgresFunctionExpressionVisitor(
            new PostgresDataAccessorIdentifierExpressionVisitor(
                new PostgresConstantExpressionVisitor(this)));

    String value = expression.getExpression().accept(selectTypeExpressionVisitor);
    return value != null ? convertToAggregationFunction(operator, value) : null;
  }

  private String convertToAggregationFunction(AggregationOperator operator, String value) {
    if (operator.equals(AggregationOperator.DISTINCT_COUNT)) {
      return String.format("COUNT(DISTINCT %s )", value);
    }
    return String.format("%s( %s )", operator, value);
  }
}
