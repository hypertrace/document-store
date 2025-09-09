package org.hypertrace.core.documentstore.postgres.registry;

import java.util.Objects;

/**
 * Immutable data class representing metadata about a PostgreSQL column. Contains the column name
 * and its corresponding PostgreSQL data type.
 */
public final class PostgresColumnInfo {

  private final String columnName;
  private final PostgresColumnType columnType;

  /**
   * Creates a new PostgresColumnInfo instance.
   *
   * @param columnName the PostgreSQL column name (must not be null)
   * @param columnType the PostgreSQL column type (must not be null)
   * @throws IllegalArgumentException if columnName or columnType is null
   */
  public PostgresColumnInfo(String columnName, PostgresColumnType columnType) {
    this.columnName = Objects.requireNonNull(columnName, "Column name cannot be null");
    this.columnType = Objects.requireNonNull(columnType, "Column type cannot be null");
  }

  /**
   * Gets the PostgreSQL column name.
   *
   * @return the column name
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * Gets the PostgreSQL column type.
   *
   * @return the column type
   */
  public PostgresColumnType getColumnType() {
    return columnType;
  }

  /**
   * Checks if this column represents a first-class field (non-JSONB).
   *
   * @return true if this is a first-class field, false if it's a JSONB field
   */
  public boolean isFirstClassField() {
    return columnType.isFirstClassField();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PostgresColumnInfo that = (PostgresColumnInfo) obj;
    return Objects.equals(columnName, that.columnName) && columnType == that.columnType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, columnType);
  }

  @Override
  public String toString() {
    return String.format(
        "PostgresColumnInfo{columnName='%s', columnType=%s}", columnName, columnType);
  }
}
