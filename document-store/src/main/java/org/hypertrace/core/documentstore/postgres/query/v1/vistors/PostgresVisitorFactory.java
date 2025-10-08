package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import javax.annotation.Nullable;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

/**
 * Factory for creating appropriate Postgres visitor instances based on collection type (Flat vs
 * Nested).
 *
 * <p>This factory eliminates the need for scattered DocumentType checks throughout visitor
 * implementations by creating type-specific visitor instances.
 */
public class PostgresVisitorFactory {

  /**
   * Creates a filter visitor appropriate for the collection type.
   *
   * @param postgresQueryParser the query parser
   * @param wrappingVisitorProvider optional wrapping visitor provider for nested array filtering
   * @return FilterTypeExpressionVisitor implementation (Flat or Nested)
   */
  public static FilterTypeExpressionVisitor createFilterVisitor(
      PostgresQueryParser postgresQueryParser,
      @Nullable PostgresWrappingFilterVisitorProvider wrappingVisitorProvider) {

    DocumentType documentType = postgresQueryParser.getPgColTransformer().getDocumentType();

    if (documentType == DocumentType.FLAT) {
      return new PostgresFlatFilterTypeExpressionVisitor(
          postgresQueryParser, wrappingVisitorProvider);
    } else {
      return new PostgresNestedFilterTypeExpressionVisitor(
          postgresQueryParser, wrappingVisitorProvider);
    }
  }

  /**
   * Creates a FROM clause visitor appropriate for the collection type.
   *
   * @param postgresQueryParser the query parser
   * @return FromTypeExpressionVisitor implementation (Flat or Nested)
   */
  public static FromTypeExpressionVisitor createFromVisitor(
      PostgresQueryParser postgresQueryParser) {

    DocumentType documentType = postgresQueryParser.getPgColTransformer().getDocumentType();

    if (documentType == DocumentType.FLAT) {
      return new PostgresFlatFromTypeExpressionVisitor(postgresQueryParser);
    } else {
      return new PostgresNestedFromTypeExpressionVisitor(postgresQueryParser);
    }
  }

  /**
   * Creates an unnest filter visitor appropriate for the collection type.
   *
   * <p>Note: Unnest filter visitors implement FromTypeExpressionVisitor to traverse unnest
   * expressions and extract their filters.
   *
   * @param postgresQueryParser the query parser
   * @return FromTypeExpressionVisitor implementation for unnest filter operations
   */
  public static FromTypeExpressionVisitor createUnnestFilterVisitor(
      PostgresQueryParser postgresQueryParser) {

    DocumentType documentType = postgresQueryParser.getPgColTransformer().getDocumentType();

    if (documentType == DocumentType.FLAT) {
      return new PostgresFlatUnnestFilterTypeExpressionVisitor(postgresQueryParser);
    } else {
      return new PostgresNestedUnnestFilterTypeExpressionVisitor(postgresQueryParser);
    }
  }
}
