package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Transformer for legacy document-based collections where fields are stored in JSONB columns.
 * Preserves the existing logic for mapping fields to JSONB paths.
 */
public class NestedPostgresColTransformer implements PostgresColTransformer {

  private static final String DOT = ".";

  @Override
  public FieldToPgColumn transform(
      IdentifierExpression expression, Map<String, String> pgColMapping) {
    // For nested collections, use the expression name for field mapping
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

  @Override
  public String buildFieldAccessorWithCast(FieldToPgColumn fieldToPgColumn, Type type) {
    if (fieldToPgColumn.getTransformedField() == null) {
      // Direct column access, no casting needed
      return fieldToPgColumn.getPgColumn();
    }

    // JSONB field access with casting
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
    return DocumentType.NESTED;
  }
}
