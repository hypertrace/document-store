package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Map;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.FieldTransformationVisitor;
import org.hypertrace.core.documentstore.postgres.utils.BasicPostgresSecurityValidator;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Transformer for flat structure collections where all fields map directly to PostgreSQL columns.
 * Used when flatStructureCollectionName is configured.
 *
 * <p>Implements the visitor pattern to avoid instanceof checks and improve code cohesion.
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

  @Override
  public FieldToPgColumn visit(IdentifierExpression expression) {
    String fieldName = expression.getName();

    BasicPostgresSecurityValidator.getDefault().validateIdentifier(fieldName);

    // Check if this field has been unnested (e.g., "tags" -> "tags_unnested")
    String pgColumnName = pgColMapping.getOrDefault(fieldName, fieldName);

    // Validate the mapped column name as well
    BasicPostgresSecurityValidator.getDefault().validateIdentifier(pgColumnName);

    return new FieldToPgColumn(null, PostgresUtils.wrapFieldNamesWithDoubleQuotes(pgColumnName));
  }

  /**
   * Visits a JsonIdentifierExpression. For flat collections, use the explicit JSONB column and path
   * metadata.
   */
  @Override
  public FieldToPgColumn visit(JsonIdentifierExpression expression) {

    BasicPostgresSecurityValidator.getDefault().validateIdentifier(expression.getColumnName());
    BasicPostgresSecurityValidator.getDefault().validateJsonPath(expression.getJsonPath());

    String fieldName = expression.getName();

    FieldToPgColumn transformedCol;

    // Check if this JSONB field has been unnested (e.g., "props.colors" -> "props_colors_encoded")
    // If unnested, return the direct column reference to the unnested alias
    if (pgColMapping.containsKey(fieldName)) {
      String unnestColumnName = pgColMapping.get(fieldName);
      BasicPostgresSecurityValidator.getDefault().validateIdentifier(unnestColumnName);
      // Return direct column access (no JSONB path) for unnested fields
      transformedCol =
          new FieldToPgColumn(null, PostgresUtils.wrapFieldNamesWithDoubleQuotes(unnestColumnName));
    } else {
      // Not unnested - use normal JSONB accessor
      String nestedPath = String.join(".", expression.getJsonPath());
      transformedCol =
          new FieldToPgColumn(
              nestedPath, PostgresUtils.wrapFieldNamesWithDoubleQuotes(expression.getColumnName()));
    }
    return transformedCol;
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
