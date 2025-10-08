package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import javax.annotation.Nullable;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

/**
 * Filter visitor for nested Postgres collections using JSONB storage.
 *
 * <p>This visitor handles queries on collections where documents are stored as JSONB and arrays are
 * JSONB arrays accessed via JSON path operators.
 *
 * <p>Key characteristics:
 *
 * <ul>
 *   <li>Uses jsonb_array_elements() function for array operations
 *   <li>Uses COALESCE with '[]'::jsonb for null handling
 *   <li>JSONB path accessors (e.g., document->'field')
 * </ul>
 */
public class PostgresNestedFilterTypeExpressionVisitor
    extends PostgresFilterTypeExpressionVisitorBase {

  public PostgresNestedFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this(postgresQueryParser, null);
  }

  public PostgresNestedFilterTypeExpressionVisitor(
      PostgresQueryParser postgresQueryParser,
      @Nullable PostgresWrappingFilterVisitorProvider wrappingVisitorProvider) {
    super(postgresQueryParser, wrappingVisitorProvider);
  }

  @Override
  protected String buildArrayRelationalFilter(ArrayRelationalFilterExpression expression) {
    /*
    For nested collections with JSONB:
    EXISTS
     (SELECT 1
      FROM jsonb_array_elements(COALESCE(document->'tags', '[]'::jsonb)) AS tags
      WHERE TRIM('"' FROM tags::text) = 'value'
     )
    */

    final String identifierName = getIdentifierName(expression);

    // Use JSONB path accessor (e.g., document->'field')
    final PostgresIdentifierExpressionVisitor identifierVisitor =
        new PostgresIdentifierExpressionVisitor(postgresQueryParser);
    final PostgresSelectTypeExpressionVisitor arrayPathVisitor =
        wrappingVisitorProvider == null
            ? new PostgresFieldIdentifierExpressionVisitor(identifierVisitor)
            : wrappingVisitorProvider.getForNonRelational(identifierVisitor);
    final String parsedLhs = expression.getArraySource().accept(arrayPathVisitor);

    final String alias = getEncodedAlias(identifierName);

    // Build filter on unnested elements
    final PostgresWrappingFilterVisitorProvider visitorProvider =
        new PostgresArrayRelationalWrappingFilterVisitorProvider(
            postgresQueryParser, identifierName, alias);
    final String parsedFilter =
        expression
            .getFilter()
            .accept(
                new PostgresNestedFilterTypeExpressionVisitor(
                    postgresQueryParser, visitorProvider));

    // Use jsonb_array_elements() for JSONB arrays
    return String.format(
        "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(%s, '[]'::jsonb)) AS \"%s\" WHERE %s)",
        parsedLhs, alias, parsedFilter);
  }

  @Override
  protected String buildDocumentArrayFilter(DocumentArrayFilterExpression expression) {
    /*
    For nested collections with JSONB:
    EXISTS
    (SELECT 1
     FROM jsonb_array_elements(COALESCE(document->'field', '[]'::jsonb)) AS field_alias
     WHERE <filter_on_document_fields>
     )
    */

    final String identifierName = getIdentifierName(expression);

    // Use JSONB path accessor
    final PostgresIdentifierExpressionVisitor identifierVisitor =
        new PostgresIdentifierExpressionVisitor(postgresQueryParser);
    final PostgresSelectTypeExpressionVisitor arrayPathVisitor =
        wrappingVisitorProvider == null
            ? new PostgresFieldIdentifierExpressionVisitor(identifierVisitor)
            : wrappingVisitorProvider.getForNonRelational(identifierVisitor);
    final String parsedLhs = expression.getArraySource().accept(arrayPathVisitor);

    final String alias = getEncodedAlias(identifierName);

    // Build filter for document fields
    final PostgresWrappingFilterVisitorProvider wrapper =
        new PostgresDocumentArrayWrappingFilterVisitorProvider(postgresQueryParser, alias);
    final String parsedFilter =
        expression
            .getFilter()
            .accept(new PostgresNestedFilterTypeExpressionVisitor(postgresQueryParser, wrapper));

    // Use jsonb_array_elements() for JSONB arrays
    return String.format(
        "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(%s, '[]'::jsonb)) AS \"%s\" WHERE %s)",
        parsedLhs, alias, parsedFilter);
  }
}
