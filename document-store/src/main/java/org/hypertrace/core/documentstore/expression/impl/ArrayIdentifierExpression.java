package org.hypertrace.core.documentstore.expression.impl;

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

  private final DataType arrayElementType;

  ArrayIdentifierExpression(String name) {
    this(name, DataType.UNSPECIFIED);
  }

  ArrayIdentifierExpression(String name, DataType arrayElementType) {
    super(name);
    this.arrayElementType = arrayElementType;
  }

  public static ArrayIdentifierExpression of(String name) {
    return new ArrayIdentifierExpression(name);
  }

  static ArrayIdentifierExpression of(String name, DataType arrayElementType) {
    return new ArrayIdentifierExpression(name, arrayElementType);
  }

  public static ArrayIdentifierExpression ofStrings(final String name) {
    return of(name, DataType.STRING);
  }

  public static ArrayIdentifierExpression ofInts(final String name) {
    return of(name, DataType.INTEGER);
  }

  public static ArrayIdentifierExpression ofLongs(final String name) {
    return of(name, DataType.LONG);
  }

  public static ArrayIdentifierExpression ofFloats(final String name) {
    return of(name, DataType.FLOAT);
  }

  public static ArrayIdentifierExpression ofDoubles(final String name) {
    return of(name, DataType.DOUBLE);
  }

  public static ArrayIdentifierExpression ofBooleans(final String name) {
    return of(name, DataType.BOOLEAN);
  }

  public static ArrayIdentifierExpression ofTimestampsTz(final String name) {
    return of(name, DataType.TIMESTAMPTZ);
  }

  public static ArrayIdentifierExpression ofDates(final String name) {
    return of(name, DataType.DATE);
  }

  /**
   * Returns the data type of array elements.
   *
   * <p>This is used by database-specific type extractors to generate appropriate type casts.
   *
   * @return The element DataType (UNSPECIFIED if no type was explicitly set)
   */
  public DataType getElementDataType() {
    return arrayElementType;
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
