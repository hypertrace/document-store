package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

public class PostgresAggregationFilterTypeExpressionVisitor
    extends PostgresFilterTypeExpressionVisitor {

  public PostgresAggregationFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
  }

  @Override
  public String visit(final RelationalExpression expression) {
    SelectTypeExpression lhs = expression.getLhs();
    RelationalOperator operator = expression.getOperator();
    SelectTypeExpression rhs = expression.getRhs();

    // Only an identifier LHS and a constant RHS is supported as of now.
    PostgresSelectTypeExpressionVisitor lhsVisitor = new PostgresIdentifierExpressionVisitor();
    PostgresSelectTypeExpressionVisitor rhsVisitor = new PostgresConstantExpressionVisitor();

    String key = lhs.accept(lhsVisitor);
    Object value = rhs.accept(rhsVisitor);

    // In SQL, the alias is not supported in the Filter and Having clause.
    // As of now, where is clause is first parsed and it is not using any alias expression.
    // However, having clause is parsed after group by, and currently, it is only
    // supported with alias expression.

    if (!postgresQueryParser.getPgSelections().containsKey(key)) {
      throw new UnsupportedOperationException(
          "Having clause is only supported with alias expression"
              + "for aggregation and functional expression");
    }
    return PostgresUtils.prepareParsedNonCompositeFilter(
        postgresQueryParser.getPgSelections().get(key),
        operator.toString(),
        value,
        this.postgresQueryParser.getParamsBuilder());
  }

  public static Optional<String> getAggregationFilterClause(
      PostgresQueryParser postgresQueryParser) {
    return prepareFilterClause(
        postgresQueryParser.getQuery().getAggregationFilter(), postgresQueryParser);
  }

  public static Optional<String> prepareFilterClause(
      Optional<FilterTypeExpression> filterTypeExpression,
      PostgresQueryParser postgresQueryParser) {
    return filterTypeExpression.map(
        expression ->
            expression.accept(
                new PostgresAggregationFilterTypeExpressionVisitor(postgresQueryParser)));
  }
}
