package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

/**
 * Unnest filter visitor for nested Postgres collections.
 *
 * <p>For nested collections with unnest operations:
 *
 * <ul>
 *   <li>Only returns unnest-specific filters
 *   <li>Main filters are already applied in table0 (work fine on JSONB paths)
 * </ul>
 */
public class PostgresNestedUnnestFilterTypeExpressionVisitor implements FromTypeExpressionVisitor {

  private final PostgresQueryParser postgresQueryParser;

  public PostgresNestedUnnestFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
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
   * Gets the filter clause for nested collections with unnest operations.
   *
   * <p>Only returns unnest-specific filters. Main query filter is already applied in table0.
   *
   * @param postgresQueryParser the query parser
   * @return Optional filter clause
   */
  public static Optional<String> getFilterClause(PostgresQueryParser postgresQueryParser) {
    PostgresNestedUnnestFilterTypeExpressionVisitor visitor =
        new PostgresNestedUnnestFilterTypeExpressionVisitor(postgresQueryParser);

    // Get filters from unnest expressions (if any)
    String unnestFilters =
        postgresQueryParser.getQuery().getFromTypeExpressions().stream()
            .map(fromTypeExpression -> fromTypeExpression.accept(visitor))
            .map(Object::toString)
            .filter(StringUtils::isNotEmpty)
            .collect(Collectors.joining(" AND "));

    // For nested collections, only return unnest filters
    // (main filter already applied in table0)
    return StringUtils.isNotEmpty(unnestFilters) ? Optional.of(unnestFilters) : Optional.empty();
  }
}
