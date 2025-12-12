package org.hypertrace.core.documentstore.expression.impl;

/**
 * Marker interface for database-specific type metadata used in flat relational collections.
 *
 * <p>Implementations of this interface represent the native data types of specific relational
 * databases (e.g., PostgreSQL, MySQL, Oracle). Type information enables:
 *
 * <ul>
 *   <li>Type-safe query generation without runtime type inference
 *   <li>Explicit type casting in generated SQL
 *   <li>Array type derivation for array-typed fields
 * </ul>
 *
 * <p>This abstraction allows the document store to support multiple relational databases while
 * keeping expression classes database-agnostic.
 *
 * <p><b>Package-private:</b> Users should use factory methods on {@link IdentifierExpression} and
 * {@link ArrayIdentifierExpression} instead of directly referencing type enums.
 *
 * @see PostgresDataType
 * @see IdentifierExpression
 * @see ArrayIdentifierExpression
 */
interface FlatCollectionDataType {
  /**
   * Returns the database-specific type name used in SQL statements.
   *
   * @return the native type name (e.g., "text", "int4", "varchar" depending on the database)
   */
  String getType();
}
