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
 * Fetches schema metadata directly from Postgres system catalogs (pg_catalog). Todo: Composite PKs
 * are not supported yet
 */
@AllArgsConstructor
public class PostgresMetadataFetcher {

  private final PostgresClient client;

  private static final String DISCOVERY_SQL =
      "SELECT a.attname AS column_name, "
          + "t.typname AS udt_name, "
          + "NOT a.attnotnull AS is_nullable, "
          + "COALESCE(pk.is_pk, false) AS is_primary_key "
          + "FROM pg_attribute a "
          + "JOIN pg_class c ON a.attrelid = c.oid "
          + "JOIN pg_namespace n ON c.relnamespace = n.oid "
          + "JOIN pg_type t ON a.atttypid = t.oid "
          + "LEFT JOIN ( "
          + "  SELECT a2.attname, true AS is_pk "
          + "  FROM pg_index i "
          + "  JOIN pg_attribute a2 ON a2.attrelid = i.indrelid AND a2.attnum = ANY(i.indkey) "
          + "  WHERE i.indisprimary "
          + ") pk ON pk.attname = a.attname "
          + "WHERE c.relname = ? "
          + "AND n.nspname = 'public' "
          + "AND a.attnum > 0 "
          + "AND NOT a.attisdropped";

  public Map<String, PostgresColumnMetadata> fetch(String tableName) {
    Map<String, PostgresColumnMetadata> metadataMap = new HashMap<>();

    try (Connection conn = client.getPooledConnection();
        PreparedStatement ps = conn.prepareStatement(DISCOVERY_SQL)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String columnName = rs.getString("column_name");
          String udtName = rs.getString("udt_name");
          boolean isNullable = rs.getBoolean("is_nullable");
          boolean isArray = udtName != null && udtName.startsWith("_");
          String baseType = isArray ? udtName.substring(1) : udtName;
          boolean isPrimaryKey = rs.getBoolean("is_primary_key");
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
      return metadataMap;
    } catch (SQLException e) {
      throw new RuntimeException("Failed to fetch Postgres metadata for table: " + tableName, e);
    }
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
