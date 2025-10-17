package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.FieldTransformationVisitor;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Transformer for legacy document-based collections where fields are stored in JSONB columns.
 * Preserves the existing logic for mapping fields to JSONB paths.
 *
 * <p>Implements the visitor pattern to avoid instanceof checks and improve code cohesion.
 */
public class NestedPostgresColTransformer
    implements PostgresColTransformer, FieldTransformationVisitor<FieldToPgColumn> {

  private static final String DOT = ".";
  private Map<String, String> pgColMapping;

  @Override
  public FieldToPgColumn transform(
      IdentifierExpression expression, Map<String, String> pgColMapping) {
    // Use visitor pattern instead of instanceof check
    this.pgColMapping = pgColMapping;
    return expression.accept(this);
  }

  /**
   * Visits a regular IdentifierExpression. For nested collections, use the expression name for
   * field mapping with JSONB document paths.
   */
  @Override
  public FieldToPgColumn visit(IdentifierExpression expression) {
    String orgFieldName = expression.getName();

    // Preserve existing logic for JSONB document mode
    Optional<String> parentField =
        pgColMapping.keySet().stream()
            .filter(orgFieldName::startsWith)
            .max(Comparator.comparingInt(String::length));

    // if there is no parent field, we look inside the "document" column
    if (parentField.isEmpty()) {
      return new FieldToPgColumn(orgFieldName, PostgresUtils.DOCUMENT_COLUMN);
    }

    String pgColumn = pgColMapping.get(parentField.get());

    // exact match
    if (parentField.get().equals(orgFieldName)) {
      return new FieldToPgColumn(null, pgColumn);
    }

    String childField = StringUtils.removeStart(orgFieldName, parentField.get() + DOT);
    return new FieldToPgColumn(childField, pgColumn);
  }

  /**
   * Visits a JsonIdentifierExpression. JsonIdentifierExpression is not supported for nested
   * collections - it's only used for flat collections with explicit JSONB columns.
   *
   * @throws UnsupportedOperationException always, as nested collections don't use
   *     JsonIdentifierExpression
   */
  @Override
  public FieldToPgColumn visit(JsonIdentifierExpression expression) {
    throw new UnsupportedOperationException(
        "JsonIdentifierExpression is not supported for nested collections. "
            + "Use IdentifierExpression with dot notation instead (e.g., 'field.nested.path').");
  }

  @Override
  public String buildFieldAccessorWithCast(FieldToPgColumn fieldToPgColumn, Type type) {
    if (fieldToPgColumn.getTransformedField() == null) {
      // Direct column access
      return fieldToPgColumn.getPgColumn();
    }

    // JSONB field access with casting - used for filters/aggregations
    // Uses ->> for final field to extract as TEXT, then casts for numeric/boolean comparisons
    String dataAccessor =
        PostgresUtils.prepareFieldDataAccessorExpr(
            fieldToPgColumn.getTransformedField(), fieldToPgColumn.getPgColumn());
    return PostgresUtils.prepareCast(dataAccessor, type);
  }

  @Override
  public String buildFieldAccessorWithoutCast(FieldToPgColumn fieldToPgColumn) {
    if (fieldToPgColumn.getTransformedField() == null) {
      // Direct column access
      return fieldToPgColumn.getPgColumn();
    }

    // JSON field accessor for SELECT clauses - returns JSONB using -> for all fields
    return PostgresUtils.prepareFieldAccessorExpr(
            fieldToPgColumn.getTransformedField(), fieldToPgColumn.getPgColumn())
        .toString();
  }

  @Override
  public DocumentType getDocumentType() {
    return DocumentType.NESTED;
  }
}
