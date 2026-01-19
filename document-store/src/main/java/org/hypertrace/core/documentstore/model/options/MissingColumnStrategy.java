package org.hypertrace.core.documentstore.model.options;

/**
 * Strategy for handling document fields that don't match the schema during write operations. This
 * is mostly applicable to flat collections as they conform to a schema
 *
 * <p>This enum defines how the system should behave when encountering fields that either don't
 * exist in the schema or have incompatible types.
 */
public enum MissingColumnStrategy {
  /**
   * Skip unmatched fields and continue with the write operation. The document will be written with
   * only the fields that match the schema.
   */
  SKIP,

  /**
   * Throw a {@link org.hypertrace.core.documentstore.model.exception.SchemaMismatchException} when
   * a field doesn't match the schema. The write operation will fail.
   */
  THROW,
  IGNORE_DOCUMENT;

  public static MissingColumnStrategy defaultStrategy() {
    return SKIP;
  }
}
