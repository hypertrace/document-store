package org.hypertrace.core.documentstore.expression.impl;

import lombok.Getter;

/**
 * PostgreSQL data types for explicit type annotation in queries.
 *
 * <p>This enum provides type metadata for {@link IdentifierExpression} and {@link
 * ArrayIdentifierExpression} fields in flat PostgreSQL collections, enabling type-safe query
 * generation without runtime type inference.
 *
 * <p>Type names map to PostgreSQL's internal types from the pg_type system catalog. These are
 * stable and part of PostgreSQL's public API.
 *
 * <p><b>Package-private:</b> Users should use factory methods like {@code ofInt()}, {@code
 * ofString()}, {@code ofLongs()}, etc. instead of directly referencing this enum.
 *
 * @see <a href="https://www.postgresql.org/docs/current/datatype.html">PostgreSQL Data Types</a>
 */
@Getter
enum PostgresDataType implements FlatCollectionDataType {
  TEXT("text"), // Java: String
  INTEGER("int4"), // Java: int, Integer (32-bit)
  BIGINT("int8"), // Java: long, Long (64-bit)
  SMALLINT("int2"), // Java: short, Short (16-bit)
  FLOAT("float4"), // Java: float, Float (32-bit)
  DOUBLE("float8"), // Java: double, Double (64-bit)
  NUMERIC("numeric"), // Java: BigDecimal (arbitrary precision)
  BOOLEAN("bool"), // Java: boolean, Boolean
  TIMESTAMP("timestamp"),
  TIMESTAMPTZ("timestamptz"),
  DATE("date"),
  UUID("uuid"),
  JSONB("jsonb");

  private final String pgTypeName;

  PostgresDataType(String pgTypeName) {
    this.pgTypeName = pgTypeName;
  }

  @Override
  public String getType() {
    return pgTypeName;
  }

  @Override
  public String toString() {
    return pgTypeName;
  }
}
