package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Map;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.FieldTransformationVisitor;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Transformer for flat structure collections where all fields map directly to PostgreSQL columns.
 * Used when flatStructureCollectionName is configured.
 */
public class FlatPostgresFieldTransformer
    implements PostgresColTransformer, FieldTransformationVisitor<FieldToPgColumn> {

  private Map<String, String> pgColMapping;

  @Override
  public FieldToPgColumn transform(
      IdentifierExpression expression, Map<String, String> pgColMapping) {
    // Use visitor pattern instead of instanceof check
    this.pgColMapping = pgColMapping;
    return expression.accept(this);
  }

  /**
   * Visits a regular IdentifierExpression. For flat collections, treat the entire name as a direct
   * column (avoids ambiguity with column names that contain dots).
   */
  @Override
  public FieldToPgColumn visit(IdentifierExpression expression) {
    return new FieldToPgColumn(
        null, PostgresUtils.wrapFieldNamesWithDoubleQuotes(expression.getName()));
  }

  /**
   * Visits a JsonIdentifierExpression. For flat collections, use the explicit JSONB column and path
   * metadata.
   */
  @Override
  public FieldToPgColumn visit(JsonIdentifierExpression expression) {
    String nestedPath = String.join(".", expression.getJsonPath());
    return new FieldToPgColumn(
        nestedPath, PostgresUtils.wrapFieldNamesWithDoubleQuotes(expression.getColumnName()));
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
    return DocumentType.FLAT;
  }
}
