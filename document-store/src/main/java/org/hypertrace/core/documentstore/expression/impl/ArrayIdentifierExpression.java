package org.hypertrace.core.documentstore.expression.impl;

import lombok.EqualsAndHashCode;

/**
 * Represents an identifier expression for array-typed fields. This allows parsers to apply
 * array-specific logic (e.g., cardinality checks for EXISTS operators to exclude empty arrays).
 *
 * <p>Similar to {@link JsonIdentifierExpression}, this provides type information to parsers so they
 * can generate appropriate database-specific queries for array operations.
 */
@EqualsAndHashCode(callSuper = true)
public class ArrayIdentifierExpression extends IdentifierExpression {

  public ArrayIdentifierExpression(String name) {
    super(name);
  }

  public static ArrayIdentifierExpression of(String name) {
    return new ArrayIdentifierExpression(name);
  }
}
