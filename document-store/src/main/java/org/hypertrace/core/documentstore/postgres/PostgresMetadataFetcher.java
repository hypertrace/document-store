package org.hypertrace.core.documentstore.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;

/**
 * Fetches schema metadata directly from Postgres system catalogs. Hardcoded to query
 * information_schema.columns.
 */
@AllArgsConstructor
public class PostgresMetadataFetcher {

  private final PostgresDatastore datastore;

  // Hardcoded SQL for high-performance schema discovery
  private static final String DISCOVERY_SQL =
      "SELECT column_name, udt_name, is_nullable "
          + "FROM information_schema.columns "
          + "WHERE table_schema = 'public' AND table_name = ?";

  public Map<String, PostgresColumnMetadata> fetch(String tableName) {
    Map<String, PostgresColumnMetadata> metadataMap = new HashMap<>();

    try (Connection conn = datastore.getPostgresClient();
        PreparedStatement ps = conn.prepareStatement(DISCOVERY_SQL)) {

      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String columnName = rs.getString("column_name");
          String udtName = rs.getString("udt_name");
          boolean isNullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"));
          metadataMap.put(
              columnName,
              new PostgresColumnMetadata(
                  columnName,
                  mapToCanonicalType(udtName),
                  mapToPostgresType(udtName),
                  udtName,
                  isNullable));
        }
      }
      return metadataMap;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to fetch Postgres metadata for table: " + tableName, e);
    }
  }

  /**
   * Maps Postgres udt_name to canonical DataType.
   */
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
      default:
        return DataType.UNSPECIFIED;
    }
  }

  /**
   * Maps Postgres udt_name to PostgresDataType.
   */
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
      default:
        return PostgresDataType.UNKNOWN;
    }
  }
}
