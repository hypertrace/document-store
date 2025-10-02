package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Map;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Interface for transforming logical field names to PostgreSQL column representations.
 * Implementations handle different storage patterns (top-level columns vs JSONB nested fields).
 */
public interface PostgresColTransformer {

  /**
   * Transforms a logical field name to PostgreSQL column information.
   *
   * @param orgFieldName The original logical field name (e.g., "API.serviceName")
   * @param pgColMapping
   * @return FieldToPgColumn containing the PostgreSQL column name and optional nested path
   */
  FieldToPgColumn transform(String orgFieldName, Map<String, String> pgColMapping);

  /**
   * Builds a complete field reference expression for use in SQL queries.
   *
   * @param fieldToPgColumn The result of field transformation
   * @param type The SQL type to cast the field to (if needed)
   * @return Complete SQL expression for accessing the field
   */
  String buildFieldAccessorWithCast(FieldToPgColumn fieldToPgColumn, Type type);

  /**
   * Builds a field accessor expression without type casting. Used for JSON field access where the
   * result should remain as JSON.
   *
   * @param fieldToPgColumn The result of field transformation
   * @return SQL expression for accessing the field without casting
   */
  String buildFieldAccessorWithoutCast(FieldToPgColumn fieldToPgColumn);
}
