package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Map;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Transformer for flat structure collections where all fields map directly to PostgreSQL columns.
 * Used when flatStructureCollectionName is configured.
 */
public class FlatPostgresFieldTransformer implements PostgresColTransformer {

  @Override
  public FieldToPgColumn transform(
      IdentifierExpression expression, Map<String, String> pgColMapping) {
    // For plain IdentifierExpression, treat the entire name as a direct column
    // This avoids ambiguity with column names that contain dots
    return new FieldToPgColumn(
        null, PostgresUtils.wrapFieldNamesWithDoubleQuotes(expression.getName()));
  }

  public FieldToPgColumn transform(
      JsonIdentifierExpression expression, Map<String, String> pgColMapping) {
    // Use the explicit metadata from JsonIdentifierExpression
    String nestedPath = String.join(".", expression.getJsonPath());
    return new FieldToPgColumn(
        nestedPath, PostgresUtils.wrapFieldNamesWithDoubleQuotes(expression.getColumnName()));
  }

  @Override
  public String buildFieldAccessorWithCast(FieldToPgColumn fieldToPgColumn, Type type) {
    if (fieldToPgColumn.getTransformedField() == null) {
      // Direct column access, no casting needed
      return fieldToPgColumn.getPgColumn();
    }

    // JSONB field access with casting
    // For STRING: use ->> (text accessor) with cast
    // For STRING_ARRAY: use -> (jsonb accessor) to keep as jsonb
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

    // JSON field accessor (without casting)
    return PostgresUtils.prepareFieldAccessorExpr(
            fieldToPgColumn.getTransformedField(), fieldToPgColumn.getPgColumn())
        .toString();
  }

  @Override
  public DocumentType getDocumentType() {
    return DocumentType.FLAT;
  }
}
