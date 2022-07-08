package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;

@NoArgsConstructor
public class PostgresAggregateExpressionVisitor extends PostgresSelectTypeExpressionVisitor {

  public PostgresAggregateExpressionVisitor(PostgresSelectTypeExpressionVisitor baseVisitor) {
    super(baseVisitor);
  }

  @Override
  public String visit(final AggregateExpression expression) {
    AggregationOperator operator = expression.getAggregator();
    PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor =
        new PostgresFunctionExpressionVisitor(
            new PostgresDataAccessorIdentifierExpressionVisitor(
                new PostgresConstantExpressionVisitor(this)));

    String value = expression.getExpression().accept(selectTypeExpressionVisitor);
    return value != null ? String.format("%s( %s )", operator, value) : null;
  }
}
