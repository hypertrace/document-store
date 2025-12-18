package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.DataType;

/**
 * PostgreSQL-specific data types with their SQL type strings.
 *
 * <p>This enum maps generic {@link DataType} values to PostgreSQL-specific type strings used in SQL
 * queries for type casting.
 */
public enum PostgresDataType {
  TEXT("text"),
  INTEGER("integer"),
  BIGINT("bigint"),
  REAL("real"),
  DOUBLE_PRECISION("double precision"),
  BOOLEAN("boolean"),
  TIMESTAMPTZ("timestamptz"),
  DATE("date"),
  UNKNOWN("unknown");

  private final String sqlType;

  PostgresDataType(String sqlType) {
    this.sqlType = sqlType;
  }

  public String getSqlType() {
    return sqlType;
  }

  public String getArraySqlType() {
    return sqlType + "[]";
  }

  /**
   * Maps a generic DataType to its PostgreSQL equivalent.
   *
   * @param dataType the generic data type
   * @return the corresponding PostgresDataType, or null if UNSPECIFIED
   * @throws IllegalArgumentException if the DataType is unknown
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
}
