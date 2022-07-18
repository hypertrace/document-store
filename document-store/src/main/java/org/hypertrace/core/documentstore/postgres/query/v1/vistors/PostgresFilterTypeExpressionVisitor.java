package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FieldToPgColumn;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

public class PostgresFilterTypeExpressionVisitor implements FilterTypeExpressionVisitor {

  protected PostgresQueryParser postgresQueryParser;

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
    PostgresSelectTypeExpressionVisitor lhsVisitor = new PostgresIdentifierExpressionVisitor();
    PostgresSelectTypeExpressionVisitor rhsVisitor = new PostgresConstantExpressionVisitor();

    String fieldName = lhs.accept(lhsVisitor);
    Object value = rhs.accept(rhsVisitor);

    FieldToPgColumn fieldToPgColumn =
        postgresQueryParser.getToPgColumnTransformer().transform(fieldName);

    return PostgresUtils.parseNonCompositeFilter(
        fieldToPgColumn.getTransformedField(),
        fieldToPgColumn.getPgColumn(),
        operator.toString(),
        value,
        postgresQueryParser.getParamsBuilder());
  }

  public static Optional<String> getFilterClause(PostgresQueryParser postgresQueryParser) {
    return prepareFilterClause(postgresQueryParser.getQuery().getFilter(), postgresQueryParser);
  }

  public static Optional<String> prepareFilterClause(
      Optional<FilterTypeExpression> filterTypeExpression,
      PostgresQueryParser postgresQueryParser) {
    return filterTypeExpression.map(
        expression ->
            expression.accept(new PostgresFilterTypeExpressionVisitor(postgresQueryParser)));
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
