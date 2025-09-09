package org.hypertrace.core.documentstore.postgres.registry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.postgres.PostgresTableIdentifier;

/**
 * Implementation of PostgresColumnRegistry that queries the database schema to build column type
 * mappings dynamically.
 *
 * <p>This implementation queries the information_schema.columns table to discover the actual column
 * types in the PostgreSQL table and maps them to the appropriate PostgresColumnType enum values.
 */
@Slf4j
public class PostgresColumnRegistryImpl implements PostgresColumnRegistry {

  private final String tableName;
  private final Map<String, PostgresColumnInfo> columnMappings;

  /**
   * Creates a new PostgresColumnRegistryImpl by querying the database schema.
   *
   * @param connection the database connection to use for schema queries
   * @param tableIdentifier the table identifier to query schema for
   * @throws SQLException if there's an error querying the database schema
   */
  public PostgresColumnRegistryImpl(Connection connection, PostgresTableIdentifier tableIdentifier)
      throws SQLException {
    this.tableName = tableIdentifier.getTableName();
    this.columnMappings = buildColumnMappings(connection, tableIdentifier);
    System.out.println("-- PostgresColumnRegistryImpl --");
    System.out.printf("Table Name: %s\n", this.tableName);
    System.out.println(this.columnMappings);
    System.out.println("-- end --");
    System.out.println();

    log.debug(
        "Created PostgresColumnRegistry for table '{}' with {} first-class columns: {}",
        tableName,
        columnMappings.size(),
        columnMappings.keySet());
  }

  /**
   * Creates a new PostgresColumnRegistryImpl by querying the database schema.
   *
   * @param connection the database connection to use for schema queries
   * @param collectionName the collection name (table name) to query schema for
   * @throws SQLException if there's an error querying the database schema
   */
  public PostgresColumnRegistryImpl(Connection connection, String collectionName)
      throws SQLException {
    this(connection, PostgresTableIdentifier.parse(collectionName));
  }

  @Override
  public boolean isFirstClassColumn(String fieldName) {
    return columnMappings.containsKey(fieldName);
  }

  @Override
  public Optional<PostgresColumnType> getColumnType(String fieldName) {
    PostgresColumnInfo columnInfo = columnMappings.get(fieldName);
    return columnInfo != null ? Optional.of(columnInfo.getColumnType()) : Optional.empty();
  }

  @Override
  public Optional<String> getColumnName(String fieldName) {
    PostgresColumnInfo columnInfo = columnMappings.get(fieldName);
    return columnInfo != null ? Optional.of(columnInfo.getColumnName()) : Optional.empty();
  }

  @Override
  public Set<String> getAllFirstClassColumns() {
    return Collections.unmodifiableSet(columnMappings.keySet());
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  /**
   * Builds the column mappings by querying the database schema.
   *
   * @param connection the database connection
   * @param tableIdentifier the table identifier
   * @return a map of field names to PostgresColumnInfo objects
   * @throws SQLException if there's an error querying the database
   */
  private Map<String, PostgresColumnInfo> buildColumnMappings(
      Connection connection, PostgresTableIdentifier tableIdentifier) throws SQLException {

    Map<String, PostgresColumnInfo> mappings = new HashMap<>();

    String query =
        "SELECT column_name, data_type, udt_name "
            + "FROM information_schema.columns "
            + "WHERE table_name = ? AND table_schema = ? "
            + "ORDER BY ordinal_position";

    String schemaName = tableIdentifier.getSchema().orElse("public");
    String tableName = tableIdentifier.getTableName();

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, tableName);
      stmt.setString(2, schemaName);

      log.debug("Querying schema for table: {}.{}", schemaName, tableName);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs != null) {
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String dataType = rs.getString("data_type");
            String udtName = rs.getString("udt_name");

            PostgresColumnType columnType = PostgresColumnType.fromPostgresType(dataType, udtName);

            // Only include first-class columns (non-JSONB) in the registry
            if (columnType.isFirstClassField()) {
              PostgresColumnInfo columnInfo = new PostgresColumnInfo(columnName, columnType);
              mappings.put(columnName, columnInfo);

              log.debug(
                  "Registered first-class column: {} -> {} ({})", columnName, columnType, dataType);
            } else {
              log.debug("Skipping JSONB column: {} ({})", columnName, dataType);
            }
          }
        }
      }
    } catch (SQLException e) {
      log.error(
          "Failed to query schema for table {}.{}: {}", schemaName, tableName, e.getMessage());
      throw e;
    }

    return mappings;
  }

  @Override
  public String toString() {
    return String.format(
        "PostgresColumnRegistryImpl{tableName='%s', firstClassColumns=%d}",
        tableName, columnMappings.size());
  }
}
