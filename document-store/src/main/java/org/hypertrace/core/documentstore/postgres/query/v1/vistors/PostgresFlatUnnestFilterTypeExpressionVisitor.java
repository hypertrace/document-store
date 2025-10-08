package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

/**
 * Unnest filter visitor for flat Postgres collections.
 *
 * <p>For flat collections with unnest operations:
 *
 * <ul>
 *   <li>Combines unnest-specific filters with main query filters
 *   <li>Main filters are applied here (not in table0) to avoid type mismatches
 * </ul>
 */
public class PostgresFlatUnnestFilterTypeExpressionVisitor implements FromTypeExpressionVisitor {

  private final PostgresQueryParser postgresQueryParser;

  public PostgresFlatUnnestFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
  }

  @Override
  public String visit(UnnestExpression unnestExpression) {
    Optional<String> where =
        PostgresFilterTypeExpressionVisitorBase.prepareFilterClause(
            Optional.ofNullable(unnestExpression.getFilterTypeExpression()), postgresQueryParser);
    return where.orElse("");
  }

  @Override
  public String visit(SubQueryJoinExpression subQueryJoinExpression) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  /**
   * Gets the filter clause for flat collections with unnest operations.
   *
   * <p>Combines:
   *
   * <ul>
   *   <li>Unnest-specific filters (from UnnestExpression.filterTypeExpression)
   *   <li>Main query filter (from Query.filter) - applied here instead of table0
   * </ul>
   *
   * @param postgresQueryParser the query parser
   * @return Optional filter clause
   */
  public static Optional<String> getFilterClause(PostgresQueryParser postgresQueryParser) {
    PostgresFlatUnnestFilterTypeExpressionVisitor visitor =
        new PostgresFlatUnnestFilterTypeExpressionVisitor(postgresQueryParser);

    // Get filters from unnest expressions (if any)
    String unnestFilters =
        postgresQueryParser.getQuery().getFromTypeExpressions().stream()
            .map(fromTypeExpression -> fromTypeExpression.accept(visitor))
            .map(Object::toString)
            .filter(StringUtils::isNotEmpty)
            .collect(Collectors.joining(" AND "));

    // For flat collections, we need to include the main query filter here
    // because it was skipped in table0 (to avoid type mismatches on array columns)
    Optional<String> mainFilter =
        PostgresFilterTypeExpressionVisitorBase.prepareFilterClause(
            postgresQueryParser.getQuery().getFilter(), postgresQueryParser);

    if (StringUtils.isNotEmpty(unnestFilters) && mainFilter.isPresent()) {
      return Optional.of(String.format("%s AND %s", unnestFilters, mainFilter.get()));
    } else if (StringUtils.isNotEmpty(unnestFilters)) {
      return Optional.of(unnestFilters);
    } else {
      return mainFilter;
    }
  }
}
