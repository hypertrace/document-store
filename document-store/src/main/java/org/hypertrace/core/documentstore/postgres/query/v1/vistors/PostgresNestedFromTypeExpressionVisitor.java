package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

/**
 * FROM clause visitor for nested Postgres collections using JSONB storage.
 *
 * <p>Key differences from flat collections:
 *
 * <ul>
 *   <li>Uses jsonb_array_elements() function for JSONB arrays
 *   <li>Uses JSONB path accessors (document->'field')
 *   <li>Applies filters in table0 (filters work on JSONB paths)
 * </ul>
 */
public class PostgresNestedFromTypeExpressionVisitor extends PostgresFromTypeExpressionVisitorBase {

  private static final String TABLE0_QUERY_FMT_WHERE = "table0 as (SELECT * from %s WHERE %s),";
  private static final String JSONB_UNWIND_EXP_FMT = "jsonb_array_elements(%s)";

  private final PostgresFieldIdentifierExpressionVisitor postgresFieldIdentifierExpressionVisitor;

  public PostgresNestedFromTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    super(postgresQueryParser);
    this.postgresFieldIdentifierExpressionVisitor =
        new PostgresFieldIdentifierExpressionVisitor(postgresQueryParser);
  }

  @Override
  protected UnnestConfig buildUnnestConfig(
      UnnestExpression unnestExpression, String orgFieldName, String pgColumnName) {

    // For nested collections, use JSONB path accessor
    String transformedFieldName =
        unnestExpression.getIdentifierExpression().accept(postgresFieldIdentifierExpressionVisitor);

    // Use jsonb_array_elements() for JSONB arrays
    String unnestFunction = String.format(JSONB_UNWIND_EXP_FMT, transformedFieldName);

    // Keep original column name (no suffix needed for JSONB)
    return new UnnestConfig(transformedFieldName, unnestFunction, pgColumnName);
  }

  @Override
  protected String buildTable0Query(PostgresQueryParser postgresQueryParser) {
    /*
     * For nested collections, filters work fine in table0 because they reference
     * JSONB paths in the 'document' column. Apply filters as usual.
     */
    Optional<String> whereFilter =
        PostgresFilterTypeExpressionVisitorBase.prepareFilterClause(
            postgresQueryParser.getQuery().getFilter(), postgresQueryParser);

    return whereFilter.isPresent()
        ? String.format(
            TABLE0_QUERY_FMT_WHERE, postgresQueryParser.getTableIdentifier(), whereFilter.get())
        : String.format(TABLE0_QUERY_FMT, postgresQueryParser.getTableIdentifier());
  }

  /**
   * Static method to get FROM clause for nested collections.
   *
   * @param postgresQueryParser the query parser
   * @return Optional FROM clause
   */
  public static Optional<String> getFromClause(PostgresQueryParser postgresQueryParser) {
    // Check if there are any unnest operations
    if (postgresQueryParser.getQuery().getFromTypeExpressions().isEmpty()) {
      return Optional.empty();
    }

    PostgresNestedFromTypeExpressionVisitor visitor =
        new PostgresNestedFromTypeExpressionVisitor(postgresQueryParser);
    return Optional.of(buildFromClause(postgresQueryParser, visitor));
  }
}
