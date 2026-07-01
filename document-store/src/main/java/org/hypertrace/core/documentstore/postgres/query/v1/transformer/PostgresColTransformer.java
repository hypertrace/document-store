package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import java.util.Map;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

/**
 * Interface for transforming logical field names to PostgreSQL column representations.
 * Implementations handle different storage patterns (nested vs flat).
 */
public interface PostgresColTransformer {

  /**
   * Transforms an identifier expression to PostgreSQL column information.
   *
   * @param expression The identifier expression (may be JsonIdentifierExpression with metadata)
   * @param pgColMapping Mapping of field names to PostgreSQL columns
   * @return FieldToPgColumn containing the PostgreSQL column name and optional nested path
   */
  FieldToPgColumn transform(IdentifierExpression expression, Map<String, String> pgColMapping);

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

  /**
   * Returns the kind of document this transformer is handling - Flat vs nested
   *
   * @return the corresponding document type
   */
  DocumentType getDocumentType();
}
