package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

public class PostgresFilterTypeExpressionVisitor implements FilterTypeExpressionVisitor {

  private PostgresQueryParser postgresQueryParser;

  public PostgresFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
  }

  @Override
  public <T> T visit(final LogicalExpression expression) {
    throw new UnsupportedOperationException("TODO: not yet supported");
  }

  @Override
  public String visit(final RelationalExpression expression) {
    SelectTypeExpression lhs = expression.getLhs();
    RelationalOperator operator = expression.getOperator();
    SelectTypeExpression rhs = expression.getRhs();

    // Only an identifier LHS and a constant RHS is supported as of now.
    PostgresSelectTypeExpressionVisitor lhsParser = new PostgresIdentifierExpressionVisitor();
    PostgresSelectTypeExpressionVisitor rhsParser = new PostgresConstantExpressionVisitor();

    String key = lhs.accept(lhsParser);
    Object value = rhs.accept(rhsParser);

    return PostgresUtils.parseNonCompositeFilter(
        key, operator.toString(), value, this.postgresQueryParser.getParamsBuilder());
  }
}
