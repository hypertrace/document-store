package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

public class PostgresGroupTypeExpressionVisitor implements GroupTypeExpressionVisitor {

  PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor =
      new PostgresFieldIdentifierExpressionVisitor();

  @Override
  public String visit(final FunctionExpression expression) {
    throw new UnsupportedOperationException(
        "FunctionalExpression on Group by is not yet supported");
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    return selectTypeExpressionVisitor.visit(expression);
  }

  public static String getGroupByClause(final Query query) {
    if (query.getAggregations().size() <= 0) return null;

    if (!validate(query)) {
      throw new UnsupportedOperationException("Group By clause with DISTINCT is not yet supported");
    }

    List<GroupTypeExpression> groupTypeExpressions = query.getAggregations();

    PostgresGroupTypeExpressionVisitor groupTypeExpressionVisitor =
        new PostgresGroupTypeExpressionVisitor();

    String childList =
        groupTypeExpressions.stream()
            .map(exp -> exp.accept(groupTypeExpressionVisitor))
            .filter(Objects::nonNull)
            .map(Object::toString)
            .filter(StringUtils::isNotEmpty)
            .collect(Collectors.joining(","));

    return !childList.isEmpty() ? childList : null;
  }

  private static boolean validate(Query query) {
    return !query.getSelections().stream()
        .filter(exp -> exp.getExpression() instanceof AggregateExpression)
        .anyMatch(
            exp ->
                ((AggregateExpression) exp.getExpression())
                    .getAggregator()
                    .equals(AggregationOperator.DISTINCT));
  }
}
