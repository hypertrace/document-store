package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import javax.annotation.Nullable;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

/**
 * Filter visitor for flat Postgres collections using native PostgreSQL array columns.
 *
 * <p>This visitor handles queries on collections where arrays are stored as native PostgreSQL array
 * types (e.g., TEXT[], INTEGER[]) rather than JSONB.
 *
 * <p>Key characteristics:
 *
 * <ul>
 *   <li>Uses unnest() function for array operations
 *   <li>Uses COALESCE with ARRAY[] for null handling
 *   <li>Direct column references with proper quoting
 * </ul>
 */
public class PostgresFlatFilterTypeExpressionVisitor
    extends PostgresFilterTypeExpressionVisitorBase {

  public PostgresFlatFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this(postgresQueryParser, null);
  }

  public PostgresFlatFilterTypeExpressionVisitor(
      PostgresQueryParser postgresQueryParser,
      @Nullable PostgresWrappingFilterVisitorProvider wrappingVisitorProvider) {
    super(postgresQueryParser, wrappingVisitorProvider);
  }

  @Override
  protected String buildArrayRelationalFilter(ArrayRelationalFilterExpression expression) {
    /*
    For flat collections with native arrays:
    EXISTS
     (SELECT 1
      FROM unnest(COALESCE("tags", ARRAY[])) AS tags_unnested
      WHERE tags_unnested = 'value'
     )
    */

    final String identifierName = getIdentifierName(expression);
    // Use transformer to get proper column name for native arrays
    final String parsedLhs = postgresQueryParser.transformField(identifierName).getPgColumn();
    final String alias = getEncodedAlias(identifierName);

    // Build filter on unnested elements
    final PostgresWrappingFilterVisitorProvider visitorProvider =
        new PostgresArrayRelationalWrappingFilterVisitorProvider(
            postgresQueryParser, identifierName, alias);
    final String parsedFilter =
        expression
            .getFilter()
            .accept(
                new PostgresFlatFilterTypeExpressionVisitor(postgresQueryParser, visitorProvider));

    // Use native unnest() for PostgreSQL arrays
    return String.format(
        "EXISTS (SELECT 1 FROM unnest(COALESCE(%s, ARRAY[])) AS \"%s\" WHERE %s)",
        parsedLhs, alias, parsedFilter);
  }

  @Override
  protected String buildDocumentArrayFilter(DocumentArrayFilterExpression expression) {
    /*
    For flat collections with native arrays (rare case - usually JSONB for document arrays):
    EXISTS
     (SELECT 1
      FROM unnest(COALESCE("field", ARRAY[])) AS field_alias
      WHERE <filter_on_document_fields>
     )
    */

    final String identifierName = getIdentifierName(expression);
    // Direct column reference with double quotes for flat collections
    final String parsedLhs = PostgresUtils.wrapFieldNamesWithDoubleQuotes(identifierName);
    final String alias = getEncodedAlias(identifierName);

    // Build filter for document fields
    final PostgresWrappingFilterVisitorProvider wrapper =
        new PostgresDocumentArrayWrappingFilterVisitorProvider(postgresQueryParser, alias);
    final String parsedFilter =
        expression
            .getFilter()
            .accept(new PostgresFlatFilterTypeExpressionVisitor(postgresQueryParser, wrapper));

    // Use native unnest() - Note: This is unusual for flat collections
    // Document arrays would typically be stored as JSONB even in flat collections
    return String.format(
        "EXISTS (SELECT 1 FROM unnest(COALESCE(%s, ARRAY[])) AS \"%s\" WHERE %s)",
        parsedLhs, alias, parsedFilter);
  }
}
