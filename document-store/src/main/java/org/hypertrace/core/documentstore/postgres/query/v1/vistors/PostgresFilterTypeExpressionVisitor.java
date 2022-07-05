package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
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
  public String visit(final LogicalExpression expression) {
    Collector<String, ?, String> collector =
        getCollectorForLogicalOperator(expression.getOperator());
    String childList =
        expression.getOperands().stream()
            .map(exp -> exp.accept(this))
            .filter(str -> !StringUtils.isEmpty((String) str))
            .map(str -> "(" + str + ")")
            .collect(collector);
    return !childList.isEmpty() ? childList : null;
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

  private Collector getCollectorForLogicalOperator(LogicalOperator operator) {
    if (operator.equals(LogicalOperator.OR)) {
      return Collectors.joining(" OR ");
    } else if (operator.equals(LogicalOperator.AND)) {
      return Collectors.joining(" AND ");
    }
    throw new UnsupportedOperationException(
        String.format("Query operation:%s not supported", operator));
  }
}
