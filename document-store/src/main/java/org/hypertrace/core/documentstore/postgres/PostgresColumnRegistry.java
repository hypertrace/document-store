package org.hypertrace.core.documentstore.postgres;

import java.util.Optional;
import java.util.Set;

/**
 * Registry for PostgreSQL column metadata that provides type-aware query generation support.
 *
 * <p>This interface replaces hardcoded column checks with dynamic database metadata lookups,
 * enabling support for typed columns (String, Long, Double, Boolean, TextArray) while maintaining
 * JSONB fallback for complex types.
 */
public interface PostgresColumnRegistry {

  /**
   * Determines if a field should be treated as a first-class typed column rather than a JSONB
   * document field.
   *
   * @param fieldName the field name to check
   * @return true if the field has a first-class column mapping, false if it should use JSONB
   *     processing
   */
  boolean isFirstClassColumn(String fieldName);

  /**
   * Gets the PostgreSQL data type for a field.
   *
   * @param fieldName the field name to look up
   * @return Optional containing the data type if mapped, empty if not found
   */
  Optional<PostgresDataType> getColumnDataType(String fieldName);

  /**
   * Gets all field names that have first-class column mappings.
   *
   * @return set of all first-class column field names
   */
  Set<String> getAllFirstClassColumns();

  /**
   * Checks if the registry supports a specific PostgreSQL data type.
   *
   * @param dataType the data type to check
   * @return true if the data type is supported for first-class column processing
   */
  boolean supportsDataType(PostgresDataType dataType);
}
