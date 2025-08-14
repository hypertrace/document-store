package org.hypertrace.core.documentstore.postgres.registry;

import java.util.Optional;
import java.util.Set;

/**
 * Registry interface for PostgreSQL column type information. Provides metadata about table columns
 * to determine appropriate query generation strategies.
 *
 * <p>This registry replaces hardcoded column lists and enables dynamic, database-driven decisions
 * about whether to use JSON-based or native PostgreSQL column access.
 */
public interface PostgresColumnRegistry {

  /**
   * Determines if the specified field name corresponds to a first-class column (i.e., a native
   * PostgreSQL column rather than a field within a JSONB document).
   *
   * @param fieldName the field name to check
   * @return true if this is a first-class column, false if it should be accessed via JSONB
   */
  boolean isFirstClassColumn(String fieldName);

  /**
   * Gets the PostgreSQL column type for the specified field name.
   *
   * @param fieldName the field name to look up
   * @return the PostgreSQL column type, or empty if the field is not a first-class column
   */
  Optional<PostgresColumnType> getColumnType(String fieldName);

  /**
   * Gets the actual PostgreSQL column name for the specified field name. In most cases, this will
   * be the same as the field name, but this method allows for potential field name transformations
   * or aliases.
   *
   * @param fieldName the field name to look up
   * @return the PostgreSQL column name, or empty if the field is not a first-class column
   */
  Optional<String> getColumnName(String fieldName);

  /**
   * Gets all first-class column names in this table. This is useful for debugging and validation
   * purposes.
   *
   * @return a set of all first-class column names
   */
  Set<String> getAllFirstClassColumns();

  /**
   * Gets the table name this registry represents.
   *
   * @return the table name
   */
  String getTableName();

  /**
   * Checks if this registry has any first-class columns. If false, all fields should be accessed
   * via JSONB.
   *
   * @return true if there are first-class columns, false otherwise
   */
  default boolean hasFirstClassColumns() {
    return !getAllFirstClassColumns().isEmpty();
  }
}
