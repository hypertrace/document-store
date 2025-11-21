package org.hypertrace.core.documentstore.expression.impl;

/**
 * Represents the type of a JSON field for query optimization with proper type casting.
 *
 * <p>This allows parsers to generate optimized SQL with appropriate PostgreSQL type casting:
 *
 * <ul>
 *   <li><b>Primitives:</b> STRING, NUMBER, BOOLEAN - use {@code ->>} with optional casting
 *   <li><b>Typed Arrays:</b> STRING_ARRAY, NUMBER_ARRAY, BOOLEAN_ARRAY - use {@code ->} and
 *       {@code @>} with typed jsonb_build_array
 *   <li><b>Complex:</b> OBJECT_ARRAY, OBJECT - use {@code ->} and {@code @>} containment
 * </ul>
 */
public enum JsonFieldType {
  // ==================== Primitives ====================
  /**
   * String JSON value.
   *
   * <p>Example SQL: {@code "document" ->> 'item' IN ('Soap', 'Shampoo')}
   */
  STRING,

  /**
   * Numeric JSON value (integer, decimal, etc.).
   *
   * <p>Example SQL: {@code CAST("document" ->> 'price' AS NUMERIC) IN (10, 20)}
   */
  NUMBER,

  /**
   * Boolean JSON value.
   *
   * <p>Example SQL: {@code CAST("document" ->> 'active' AS BOOLEAN) IN (true, false)}
   */
  BOOLEAN,

  // ==================== Typed Arrays ====================
  /**
   * Array of strings.
   *
   * <p>Example SQL: {@code "document" -> 'tags' @> jsonb_build_array(?::text)}
   */
  STRING_ARRAY,

  /**
   * Array of numbers.
   *
   * <p>Example SQL: {@code "document" -> 'scores' @> jsonb_build_array(?::numeric)}
   */
  NUMBER_ARRAY,

  /**
   * Array of booleans.
   *
   * <p>Example SQL: {@code "document" -> 'flags' @> jsonb_build_array(?::boolean)}
   */
  BOOLEAN_ARRAY,

  /**
   * Array of JSON objects.
   *
   * <p>Example SQL: {@code "document" -> 'items' @> jsonb_build_array(?::jsonb)}
   */
  OBJECT_ARRAY,

  // ==================== Complex Types ====================
  /**
   * JSON object/document value.
   *
   * <p>Example SQL: {@code "document" -> 'metadata' @> '{"status":"active"}'::jsonb}
   */
  OBJECT
}
