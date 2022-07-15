package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

public class PostgresUnnestFilterTypeExpressionVisitor implements FromTypeExpressionVisitor {

  private PostgresQueryParser postgresQueryParser;

  public PostgresUnnestFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
  }

  @Override
  public String visit(UnnestExpression unnestExpression) {
    Optional<String> where =
        PostgresFilterTypeExpressionVisitor.prepareFilterClause(
            Optional.ofNullable(unnestExpression.getFilterTypeExpression()), postgresQueryParser);
    return where.orElse("");
  }

  public static Optional<String> getFilterClause(PostgresQueryParser postgresQueryParser) {
    PostgresUnnestFilterTypeExpressionVisitor postgresUnnestFilterTypeExpressionVisitor =
        new PostgresUnnestFilterTypeExpressionVisitor(postgresQueryParser);
    String childList =
        postgresQueryParser.getQuery().getFromTypeExpressions().stream()
            .map(
                fromTypeExpression ->
                    fromTypeExpression.accept(postgresUnnestFilterTypeExpressionVisitor))
            .map(Object::toString)
            .filter(StringUtils::isNotEmpty)
            .collect(Collectors.joining(" AND "));
    return StringUtils.isNotEmpty(childList) ? Optional.of(childList) : Optional.empty();
  }
}
