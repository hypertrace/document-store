package org.hypertrace.core.documentstore.postgres.query.v1.parser.builder;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.getType;

import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresConstantExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresDataAccessorIdentifierExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFieldIdentifierExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresFunctionExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;

@AllArgsConstructor
public class PostgresSelectExpressionParserBuilderImpl
    implements PostgresSelectExpressionParserBuilder {

  private final PostgresQueryParser postgresQueryParser;

  @Override
  public PostgresSelectTypeExpressionVisitor build(final RelationalExpression expression) {
    switch (expression.getOperator()) {
      case CONTAINS:
      case NOT_CONTAINS:
      case EXISTS:
      case NOT_EXISTS:
      case IN:
      case NOT_IN:
        return new PostgresFunctionExpressionVisitor(
            new PostgresFieldIdentifierExpressionVisitor(this.postgresQueryParser));

      default:
        return new PostgresFunctionExpressionVisitor(
            new PostgresDataAccessorIdentifierExpressionVisitor(
                this.postgresQueryParser,
                getType(expression.getRhs().accept(new PostgresConstantExpressionVisitor()))));
    }
  }
}
