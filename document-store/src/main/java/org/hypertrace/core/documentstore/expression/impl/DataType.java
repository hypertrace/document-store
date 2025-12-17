package org.hypertrace.core.documentstore.expression.impl;

/**
 * Database-agnostic data types for explicit type annotation in queries.
 *
 * <p>This enum provides type metadata for {@link IdentifierExpression} and {@link
 * ArrayIdentifierExpression} fields in flat collections, enabling type-safe query generation
 * without runtime type inference.
 *
 * <p>These types are mapped to database-specific types at query parsing time. For example, when
 * generating PostgreSQL queries, {@code STRING} maps to {@code text}, {@code INTEGER} maps to
 * {@code int4}, etc.
 *
 * @see ArrayIdentifierExpression
 * @see IdentifierExpression
 */
public enum DataType {
  UNSPECIFIED,
  STRING,
  INTEGER,
  LONG,
  FLOAT,
  DOUBLE,
  BOOLEAN,
  TIMESTAMPTZ,
  DATE
}
