package org.hypertrace.core.documentstore.model.exception;

import java.io.IOException;

/**
 * Exception thrown when a document field doesn't match the expected schema. This can occur when:
 *
 * <ul>
 *   <li>A field in the document doesn't exist in the schema
 *   <li>A field's value type doesn't match the expected schema type
 * </ul>
 */
public class SchemaMismatchException extends IOException {

  public SchemaMismatchException(String message) {
    super(message);
  }

  public SchemaMismatchException(String message, Throwable cause) {
    super(message, cause);
  }
}
