package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.getType;

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
    PostgresSelectTypeExpressionVisitor rhsVisitor = new PostgresConstantExpressionVisitor();
    Object value = rhs.accept(rhsVisitor);
    PostgresIdentifierExpressionVisitor identifierVisitor =
        new PostgresIdentifierExpressionVisitor();
    PostgresSelectTypeExpressionVisitor lhsVisitor =
        new PostgresFunctionExpressionVisitor(
            new PostgresDataAccessorIdentifierExpressionVisitor(
                postgresQueryParser, getType(value)));

    final String parsedLhsExpression = lhs.accept(lhsVisitor);
    String fieldName;

    try {
      fieldName = lhs.accept(identifierVisitor);
    } catch (final UnsupportedOperationException e) {
      // For unsupported LHS parsers, use the extra field created by the '$addFields' stage
      fieldName = parsedLhsExpression;
    }

    FieldToPgColumn fieldToPgColumn =
        postgresQueryParser.getToPgColumnTransformer().transform(fieldName);

    if (fieldToPgColumn.getTransformedField() == null)
      throw new UnsupportedOperationException("jsonb types in where clause is not yet supported");

    return PostgresUtils.parseNonCompositeFilter(
        fieldToPgColumn.getTransformedField(),
        parsedLhsExpression,
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
