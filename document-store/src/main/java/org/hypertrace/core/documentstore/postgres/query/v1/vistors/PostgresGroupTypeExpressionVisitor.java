package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.query.Query;

public class PostgresGroupTypeExpressionVisitor implements GroupTypeExpressionVisitor {

  private PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor;

  public PostgresGroupTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    selectTypeExpressionVisitor = new PostgresFieldIdentifierExpressionVisitor(postgresQueryParser);
  }

  @Override
  public String visit(final FunctionExpression expression) {
    throw new UnsupportedOperationException(
        "FunctionalExpression on Group by is not yet supported");
  }

  @Override
  public String visit(final IdentifierExpression expression) {
    return selectTypeExpressionVisitor.visit(expression);
  }

  @Override
  public String visit(final JsonIdentifierExpression expression) {
    return selectTypeExpressionVisitor.visit(expression);
  }

  public static String getGroupByClause(PostgresQueryParser postgresQueryParser) {
    Query query = postgresQueryParser.getQuery();
    if (query.getAggregations().size() <= 0) return null;

    List<GroupTypeExpression> groupTypeExpressions = query.getAggregations();

    PostgresGroupTypeExpressionVisitor groupTypeExpressionVisitor =
        new PostgresGroupTypeExpressionVisitor(postgresQueryParser);

    String childList =
        groupTypeExpressions.stream()
            .map(exp -> exp.accept(groupTypeExpressionVisitor))
            .filter(Objects::nonNull)
            .map(Object::toString)
            .filter(StringUtils::isNotEmpty)
            .collect(Collectors.joining(","));

    return !childList.isEmpty() ? childList : null;
  }
}
