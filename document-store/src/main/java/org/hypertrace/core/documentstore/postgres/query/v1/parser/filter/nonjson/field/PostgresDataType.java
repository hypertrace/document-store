package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;

/**
 * PostgreSQL-specific data types with their SQL type strings.
 *
 * <p>This enum maps generic {@link DataType} values to PostgreSQL internal type names, which work
 * for both JDBC's {@code Connection.createArrayOf()} and SQL type casting.
 */
public enum PostgresDataType {
  TEXT("text"),
  INTEGER("int4"),
  BIGINT("int8"),
  REAL("float4"),
  DOUBLE_PRECISION("float8"),
  BOOLEAN("bool"),
  JSONB("jsonb"),
  TIMESTAMPTZ("timestamptz"),
  DATE("date"),
  UNKNOWN(null);

  private final String sqlType;

  PostgresDataType(String sqlType) {
    this.sqlType = sqlType;
  }

  /**
   * Returns the PostgreSQL type name for use with JDBC's createArrayOf() and SQL casting.
   *
   * @return The type name (e.g., "int4", "float8", "text")
   */
  public String getSqlType() {
    return sqlType;
  }

  /** Returns the array type for SQL casting (e.g., "int4[]", "text[]"). */
  public String getArraySqlType() {
    return sqlType + "[]";
  }

  public String getTypeCast() {
    return sqlType == null ? "" : "::" + sqlType;
  }

  public String getArrayTypeCast() {
    return sqlType == null ? "" : "::" + sqlType + "[]";
  }

  public static PostgresDataType fromJavaValue(Object value) {
    if (value instanceof String) {
      return TEXT;
    } else if (value instanceof Integer) {
      return INTEGER;
    } else if (value instanceof Long) {
      return BIGINT;
    } else if (value instanceof Float) {
      return REAL;
    } else if (value instanceof Double) {
      return DOUBLE_PRECISION;
    } else if (value instanceof Boolean) {
      return BOOLEAN;
    }
    return UNKNOWN;
  }

  /**
   * Maps a generic DataType to its PostgreSQL equivalent.
   *
   * @param dataType the generic data type
   * @return the corresponding PostgresDataType, or null if UNSPECIFIED
   */
  public static PostgresDataType fromDataType(DataType dataType) {
    switch (dataType) {
      case UNSPECIFIED:
        return UNKNOWN;
      case STRING:
        return TEXT;
      case INTEGER:
        return INTEGER;
      case LONG:
        return BIGINT;
      case FLOAT:
        return REAL;
      case DOUBLE:
        return DOUBLE_PRECISION;
      case BOOLEAN:
        return BOOLEAN;
      case TIMESTAMPTZ:
        return TIMESTAMPTZ;
      case DATE:
        return DATE;
      default:
        throw new IllegalArgumentException("Unknown DataType: " + dataType);
    }
  }

  /**
   * Returns the PostgreSQL type cast string for JSONB array element types.
   *
   * @param fieldType the JSON field type (must be an array type)
   * @return Type cast string (e.g., "::text", "::numeric", "::boolean", "::jsonb")
   * @throws IllegalArgumentException if fieldType is not a supported array type
   */
  public static String getJsonArrayElementTypeCast(JsonFieldType fieldType) {
    switch (fieldType) {
      case STRING_ARRAY:
        return "::text";
      case NUMBER_ARRAY:
        return "::numeric";
      case BOOLEAN_ARRAY:
        return "::boolean";
      case OBJECT_ARRAY:
        return "::jsonb";
      default:
        throw new IllegalArgumentException(
            "Unsupported array type: " + fieldType + ". Expected *_ARRAY types.");
    }
  }
}
