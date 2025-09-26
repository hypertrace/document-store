package org.hypertrace.core.documentstore.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

/**
 * Implementation of PostgresColumnRegistry that queries database metadata to build dynamic
 * field-to-datatype mappings.
 *
 * <p>This implementation replaces hardcoded OUTER_COLUMNS with dynamic database metadata discovery,
 * enabling support for typed columns while maintaining JSONB fallback compatibility.
 */
@Slf4j
public class PostgresColumnRegistryImpl implements PostgresColumnRegistry {

  private final Map<String, PostgresDataType> columnTypes;
  private final String tableName;

  /**
   * Creates a new registry by querying database metadata for the specified table.
   *
   * @param connection database connection for metadata queries
   * @param tableName the table name to analyze
   */
  public PostgresColumnRegistryImpl(Connection connection, String tableName) {
    this.tableName = tableName;
    this.columnTypes = buildColumnMappings(connection, tableName);

    log.debug(
        "Created PostgresColumnRegistry for table '{}' with {} mapped columns: {}",
        tableName,
        columnTypes.size(),
        columnTypes.keySet());
  }

  @Override
  public boolean isFirstClassColumn(String fieldName) {
    if (fieldName == null) {
      return false;
    }

    // Check dynamic registry mappings first
    if (columnTypes.containsKey(fieldName)) {
      PostgresDataType type = columnTypes.get(fieldName);
      return type.isFirstClassType();
    }

    // Fallback to original hardcoded columns for compatibility
    // This ensures existing functionality continues to work during migration
    return PostgresUtils.OUTER_COLUMNS.contains(fieldName);
  }

  @Override
  public Optional<PostgresDataType> getColumnDataType(String fieldName) {
    return Optional.ofNullable(columnTypes.get(fieldName));
  }

  @Override
  public Set<String> getAllFirstClassColumns() {
    return columnTypes.entrySet().stream()
        .filter(entry -> entry.getValue().isFirstClassType())
        .map(Map.Entry::getKey)
        .collect(java.util.stream.Collectors.toSet());
  }

  @Override
  public boolean supportsDataType(PostgresDataType dataType) {
    return dataType != null && dataType.isFirstClassType();
  }

  /**
   * Queries database metadata to build field-to-datatype mappings.
   *
   * @param connection database connection
   * @param table table name to analyze
   * @return map of field names to PostgreSQL data types
   */
  private Map<String, PostgresDataType> buildColumnMappings(Connection connection, String table) {
    Map<String, PostgresDataType> mappings = new HashMap<>();

    String sql =
        "SELECT column_name, data_type, udt_name "
            + "FROM information_schema.columns "
            + "WHERE table_name = ? "
            + "AND table_schema = current_schema() "
            + "ORDER BY ordinal_position";

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, table);

      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String columnName = rs.getString("column_name");
          String dataType = rs.getString("data_type");
          String udtName = rs.getString("udt_name");

          // Use udt_name for array types (e.g., "_text"), data_type for others
          String typeToMap = "ARRAY".equals(dataType) ? udtName : dataType;
          PostgresDataType postgresType = PostgresDataType.fromPostgresTypeName(typeToMap);

          mappings.put(columnName, postgresType);

          log.debug("Mapped column '{}' with type '{}' -> {}", columnName, typeToMap, postgresType);
        }
      }

    } catch (SQLException e) {
      log.warn("Failed to query column metadata for table '{}': {}", table, e.getMessage());
      log.debug("SQLException details", e);
      // Return empty map on error - will fall back to JSONB processing
    }

    return mappings;
  }
}
