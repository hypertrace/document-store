package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Map;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Transformer for flat structure collections where all fields map directly to PostgreSQL columns.
 * Used when flatStructureCollectionName is configured.
 */
public class FlatPostgresFieldTransformer implements PostgresColTransformer {

  @Override
  public FieldToPgColumn transform(String orgFieldName, Map<String, String> pgColMapping) {
    // Check if this field has been unnested (e.g., "tags" -> "tags_unnested")
    String pgColumnName = pgColMapping.getOrDefault(orgFieldName, orgFieldName);

    // In flat structure mode, all fields are direct PostgreSQL columns
    return new FieldToPgColumn(null, PostgresUtils.wrapFieldNamesWithDoubleQuotes(pgColumnName));
  }

  @Override
  public String buildFieldAccessorWithCast(FieldToPgColumn fieldToPgColumn, Type type) {
    // Todo: If type is JSONB, we need to prepare the field data accessor again as pgcol ->
    // transformed field (example: attributes -> triePatterns)
    return fieldToPgColumn.getPgColumn();
  }

  @Override
  public String buildFieldAccessorWithoutCast(FieldToPgColumn fieldToPgColumn) {
    // Flat structure fields are direct columns
    return fieldToPgColumn.getPgColumn();
  }

  @Override
  public DocumentType getDocumentType() {
    return DocumentType.FLAT;
  }
}
