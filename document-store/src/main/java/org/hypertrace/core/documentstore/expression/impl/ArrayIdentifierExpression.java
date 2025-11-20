package org.hypertrace.core.documentstore.expression.impl;

import java.util.Optional;
import lombok.EqualsAndHashCode;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

/**
 * Represents an identifier expression for array-typed fields. This allows parsers to apply
 * array-specific logic (e.g., cardinality checks for EXISTS operators to exclude empty arrays).
 *
 * <p>Similar to {@link JsonIdentifierExpression}, this provides type information to parsers so they
 * can generate appropriate database-specific queries for array operations.
 */
@EqualsAndHashCode(callSuper = true)
public class ArrayIdentifierExpression extends IdentifierExpression {

  private final ArrayType arrayType;

  public ArrayIdentifierExpression(String name) {
    this(name, null);
  }

  public ArrayIdentifierExpression(String name, ArrayType arrayType) {
    super(name);
    this.arrayType = arrayType;
  }

  public static ArrayIdentifierExpression of(String name) {
    return new ArrayIdentifierExpression(name);
  }

  public static ArrayIdentifierExpression of(String name, ArrayType arrayType) {
    return new ArrayIdentifierExpression(name, arrayType);
  }

  /** Returns the array type if specified, empty otherwise */
  public Optional<ArrayType> getArrayType() {
    return Optional.ofNullable(arrayType);
  }

  /**
   * Accepts a SelectTypeExpressionVisitor and dispatches to the ArrayIdentifierExpression-specific
   * visit method.
   */
  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
