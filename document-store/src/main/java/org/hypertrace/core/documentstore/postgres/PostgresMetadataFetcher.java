package org.hypertrace.core.documentstore.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

/**
 * Fetches schema metadata directly from Postgres system catalogs. Hardcoded to query
 * information_schema.columns.
 */
@AllArgsConstructor
public class PostgresMetadataFetcher {

  private final PostgresClient client;

  private static final String DISCOVERY_SQL =
      "SELECT column_name, udt_name, is_nullable "
          + "FROM information_schema.columns "
          + "WHERE table_schema = 'public' AND table_name = ?";

  private static final String PRIMARY_KEY_SQL =
      "SELECT a.attname as column_name "
          + "FROM pg_index i "
          + "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
          + "WHERE i.indrelid = ?::regclass AND i.indisprimary";

  public Map<String, PostgresColumnMetadata> fetch(String tableName) {
    Map<String, PostgresColumnMetadata> metadataMap = new HashMap<>();

    try (Connection conn = client.getPooledConnection()) {
      Set<String> primaryKeyColumns = fetchPrimaryKeyColumns(conn, tableName);

      try (PreparedStatement ps = conn.prepareStatement(DISCOVERY_SQL)) {
        ps.setString(1, tableName);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String udtName = rs.getString("udt_name");
            boolean isNullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
            boolean isArray = udtName != null && udtName.startsWith("_");
            String baseType = isArray ? udtName.substring(1) : udtName;
            boolean isPrimaryKey = primaryKeyColumns.contains(columnName);
            metadataMap.put(
                columnName,
                new PostgresColumnMetadata(
                    columnName,
                    mapToCanonicalType(baseType),
                    mapToPostgresType(baseType),
                    isNullable,
                    isArray,
                    isPrimaryKey));
          }
        }
      }
      return metadataMap;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to fetch Postgres metadata for table: " + tableName, e);
    }
  }

  private Set<String> fetchPrimaryKeyColumns(Connection conn, String tableName)
      throws SQLException {
    Set<String> pkColumns = new HashSet<>();
    try (PreparedStatement ps = conn.prepareStatement(PRIMARY_KEY_SQL)) {
      ps.setString(1, PostgresUtils.wrapFieldNamesWithDoubleQuotes(tableName));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          pkColumns.add(rs.getString("column_name"));
        }
      }
    }
    return pkColumns;
  }

  /** Maps Postgres udt_name to canonical DataType. */
  private DataType mapToCanonicalType(String udtName) {
    if (udtName == null) {
      return DataType.UNSPECIFIED;
    }

    switch (udtName.toLowerCase()) {
      case "int4":
      case "int2":
        return DataType.INTEGER;
      case "int8":
        return DataType.LONG;
      case "float4":
        return DataType.FLOAT;
      case "float8":
      case "numeric":
        return DataType.DOUBLE;
      case "bool":
        return DataType.BOOLEAN;
      case "timestamptz":
        return DataType.TIMESTAMPTZ;
      case "date":
        return DataType.DATE;
      case "text":
      case "varchar":
      case "bpchar":
      case "uuid":
        return DataType.STRING;
      case "jsonb":
        return DataType.JSON;
      default:
        return DataType.UNSPECIFIED;
    }
  }

  /** Maps Postgres udt_name to PostgresDataType. */
  private PostgresDataType mapToPostgresType(String udtName) {
    if (udtName == null) {
      return PostgresDataType.UNKNOWN;
    }

    switch (udtName.toLowerCase()) {
      case "int4":
      case "int2":
        return PostgresDataType.INTEGER;
      case "int8":
        return PostgresDataType.BIGINT;
      case "float4":
        return PostgresDataType.REAL;
      case "float8":
      case "numeric":
        return PostgresDataType.DOUBLE_PRECISION;
      case "bool":
        return PostgresDataType.BOOLEAN;
      case "timestamptz":
        return PostgresDataType.TIMESTAMPTZ;
      case "date":
        return PostgresDataType.DATE;
      case "text":
      case "varchar":
      case "bpchar":
      case "uuid":
        return PostgresDataType.TEXT;
      case "jsonb":
        return PostgresDataType.JSONB;
      default:
        return PostgresDataType.UNKNOWN;
    }
  }
}
