package org.hypertrace.core.documentstore.commons;

import java.util.Map;
import java.util.Optional;

/**
 * SchemaRegistry is an interface for a registry of schemas. This interface does not places any
 * restrictions on how schemas are loaded. They can be loaded at bootstrap and cached, or loaded
 * lazily, or via any other method.
 *
 * @param <T> the type of metadata for a particular column in the registry
 */
public interface SchemaRegistry<T extends ColumnMetadata> {

  /**
   * Returns the schema for a particular table. If the schema is not available for that table, it
   * returns null (note that some implementations may fetch the schema if this happens. The
   * interface does not make it mandatory).
   *
   * @param tableName the table for which schema has to be fetched
   * @return a map of column name to their metadata
   */
  Map<String, T> getSchema(String tableName);

  /**
   * Invalidates the current schema of the table that the schema registry is holding
   *
   * @param tableName the table name
   */
  void invalidate(String tableName);

  /**
   * Returns the metadata of a col from the registry. If the metadata is not found, an
   * implementation might fetch it from the source synchronously.
   *
   * @param tableName the table name
   * @param colName the col name
   * @return optional of the col metadata.
   */
  Optional<T> getColumnOrRefresh(String tableName, String colName);

  /**
   * Returns the primary key column name for the given table.
   *
   * @param tableName the table name
   * @return optional of the primary key column name
   */
  Optional<String> getPrimaryKeyColumn(String tableName);
}
