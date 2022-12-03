package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

public class PostgresAggregationFilterTypeExpressionVisitor
    extends PostgresFilterTypeExpressionVisitor {

  private final PostgresSelectTypeExpressionVisitor lhsFieldVisitor;
  private final PostgresSelectTypeExpressionVisitor lhsVisitor;
  private final PostgresSelectTypeExpressionVisitor rhsVisitor;

  public PostgresAggregationFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
    lhsFieldVisitor = new PostgresFieldIdentifierExpressionVisitor(postgresQueryParser);
    lhsVisitor = new PostgresIdentifierExpressionVisitor();
    rhsVisitor = new PostgresConstantExpressionVisitor();
  }

  @Override
  public String visit(final RelationalExpression expression) {
    SelectTypeExpression lhs = expression.getLhs();
    RelationalOperator operator = expression.getOperator();
    SelectTypeExpression rhs = expression.getRhs();

    // Only an identifier LHS and a constant RHS is supported as of now.
    String key = lhs.accept(lhsVisitor);
    Object value = rhs.accept(rhsVisitor);

    if (postgresQueryParser.getPgSelections().containsKey(key)) {
      return PostgresUtils.prepareParsedNonCompositeFilter(
          postgresQueryParser.getPgSelections().get(key),
          operator.toString(),
          value,
          postgresQueryParser.getParamsBuilder());
    }

    return PostgresUtils.prepareParsedNonCompositeFilter(
        PostgresUtils.prepareCastForFieldAccessor(lhs.accept(lhsFieldVisitor), value),
        operator.toString(),
        PostgresUtils.preProcessedStringForFieldAccessor(value),
        postgresQueryParser.getParamsBuilder());
  }

  @Override
  public String visit(final KeyExpression expression) {
    throw new IllegalArgumentException(
        String.format(
            "Cannot perform aggregation filtering on keys: %s", expression.getKeys().toString()));
  }

  public static Optional<String> getAggregationFilterClause(
      PostgresQueryParser postgresQueryParser) {
    Optional<FilterTypeExpression> filterTypeExpression =
        postgresQueryParser.getQuery().getAggregationFilter();
    return filterTypeExpression.map(
        expression ->
            expression.accept(
                new PostgresAggregationFilterTypeExpressionVisitor(postgresQueryParser)));
  }
}
