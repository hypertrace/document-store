package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
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

  @Override
  public String visit(SubQueryJoinExpression subQueryJoinExpression) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  public static Optional<String> getFilterClause(PostgresQueryParser postgresQueryParser) {
    PostgresUnnestFilterTypeExpressionVisitor postgresUnnestFilterTypeExpressionVisitor =
        new PostgresUnnestFilterTypeExpressionVisitor(postgresQueryParser);

    // Get filters from unnest expressions (if any)
    String unnestFilters =
        postgresQueryParser.getQuery().getFromTypeExpressions().stream()
            .map(
                fromTypeExpression ->
                    fromTypeExpression.accept(postgresUnnestFilterTypeExpressionVisitor))
            .map(Object::toString)
            .filter(StringUtils::isNotEmpty)
            .collect(Collectors.joining(" AND "));

    // For flat collections, we need to include the main query filter here
    // because it was skipped in table0 (to avoid type mismatches on array columns)
    // For nested collections, the main filter is already applied in table0,
    // so we should NOT duplicate it here
    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    if (isFlatCollection) {
      // Get main query filter and combine with unnest filters
      Optional<String> mainFilter =
          PostgresFilterTypeExpressionVisitor.prepareFilterClause(
              postgresQueryParser.getQuery().getFilter(), postgresQueryParser);

      if (StringUtils.isNotEmpty(unnestFilters) && mainFilter.isPresent()) {
        return Optional.of(String.format("%s AND %s", unnestFilters, mainFilter.get()));
      } else if (StringUtils.isNotEmpty(unnestFilters)) {
        return Optional.of(unnestFilters);
      } else {
        return mainFilter;
      }
    } else {
      // For nested collections, only return unnest filters (main filter already in table0)
      return StringUtils.isNotEmpty(unnestFilters) ? Optional.of(unnestFilters) : Optional.empty();
    }
  }
}
