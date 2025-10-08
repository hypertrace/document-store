package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

/**
 * FROM clause visitor for flat Postgres collections using native PostgreSQL array columns.
 *
 * <p>Key differences from nested collections:
 *
 * <ul>
 *   <li>Uses unnest() function for native PostgreSQL arrays
 *   <li>Appends "_unnested" suffix to column names to avoid conflicts
 *   <li>Does NOT apply filters in table0 (filters applied after unnesting)
 * </ul>
 */
public class PostgresFlatFromTypeExpressionVisitor extends PostgresFromTypeExpressionVisitorBase {

  private static final String NATIVE_UNWIND_EXP_FMT = "unnest(%s)";

  public PostgresFlatFromTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
  }

  @Override
  protected UnnestConfig buildUnnestConfig(
      UnnestExpression unnestExpression, String orgFieldName, String pgColumnName) {

    // For flat collections, assume all unnested fields are native PostgreSQL arrays
    // Use the transformer to get the proper column name (handles quotes and naming)
    String transformedFieldName = postgresQueryParser.transformField(orgFieldName).getPgColumn();

    // Use native unnest() for PostgreSQL array columns
    String unnestFunction = String.format(NATIVE_UNWIND_EXP_FMT, transformedFieldName);

    // Append "_unnested" suffix to avoid column name conflicts with the original array column
    // e.g., unnest("tags") p1(tags_unnested) instead of p1(tags)
    String finalColumnName = pgColumnName + "_unnested";

    return new UnnestConfig(transformedFieldName, unnestFunction, finalColumnName);
  }

  @Override
  protected String buildTable0Query(PostgresQueryParser postgresQueryParser) {
    /*
     * For flat collections with unnest operations, we cannot apply filters in table0 because:
     * 1. Filters on unnested fields reference scalar values that don't exist yet in table0
     * 2. Filters on array fields might use operators that don't work on arrays (like LIKE)
     *
     * Therefore, for flat collections, we skip filters in table0.
     * Filters will be applied after unnesting in the WHERE clause.
     */
    return String.format(TABLE0_QUERY_FMT, postgresQueryParser.getTableIdentifier());
  }

  /**
   * Static method to get FROM clause for flat collections.
   *
   * @param postgresQueryParser the query parser
   * @return Optional FROM clause
   */
  public static Optional<String> getFromClause(PostgresQueryParser postgresQueryParser) {
    // Check if there are any unnest operations
    if (postgresQueryParser.getQuery().getFromTypeExpressions().isEmpty()) {
      return Optional.empty();
    }

    PostgresFlatFromTypeExpressionVisitor visitor =
        new PostgresFlatFromTypeExpressionVisitor(postgresQueryParser);
    return Optional.of(buildFromClause(postgresQueryParser, visitor));
  }
}
