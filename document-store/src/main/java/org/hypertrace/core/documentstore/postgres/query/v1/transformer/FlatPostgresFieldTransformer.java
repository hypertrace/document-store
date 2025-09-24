package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Map;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Transformer for flat structure collections where all fields map directly to PostgreSQL columns.
 * Used when flatStructureCollectionName is configured.
 */
public class FlatPostgresFieldTransformer implements PostgresColTransformer {

  @Override
  public FieldToPgColumn transform(String orgFieldName, Map<String, String> pgColMapping) {
    // In flat structure mode, all fields are direct PostgreSQL columns as-is
    return new FieldToPgColumn(null, PostgresUtils.wrapFieldNamesWithDoubleQuotes(orgFieldName));
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
}
