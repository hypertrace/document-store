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

  private final FlatCollectionDataType arrayElementType;

  public ArrayIdentifierExpression(String name) {
    this(name, null);
  }

  // Package-private: used internally by factory methods
  ArrayIdentifierExpression(String name, FlatCollectionDataType arrayElementType) {
    super(name, null);
    this.arrayElementType = arrayElementType;
  }

  public static ArrayIdentifierExpression of(String name) {
    return new ArrayIdentifierExpression(name);
  }

  // Package-private: used internally by factory methods
  static ArrayIdentifierExpression of(String name, FlatCollectionDataType arrayElementType) {
    return new ArrayIdentifierExpression(name, arrayElementType);
  }

  public static ArrayIdentifierExpression ofStrings(final String name) {
    return of(name, PostgresDataType.TEXT);
  }

  public static ArrayIdentifierExpression ofInts(final String name) {
    return of(name, PostgresDataType.INTEGER);
  }

  public static ArrayIdentifierExpression ofLongs(final String name) {
    return of(name, PostgresDataType.BIGINT);
  }

  public static ArrayIdentifierExpression ofShorts(final String name) {
    return of(name, PostgresDataType.SMALLINT);
  }

  public static ArrayIdentifierExpression ofFloats(final String name) {
    return of(name, PostgresDataType.FLOAT);
  }

  public static ArrayIdentifierExpression ofDoubles(final String name) {
    return of(name, PostgresDataType.DOUBLE);
  }

  public static ArrayIdentifierExpression ofDecimals(final String name) {
    return of(name, PostgresDataType.NUMERIC);
  }

  public static ArrayIdentifierExpression ofBooleans(final String name) {
    return of(name, PostgresDataType.BOOLEAN);
  }

  public static ArrayIdentifierExpression ofTimestamps(final String name) {
    return of(name, PostgresDataType.TIMESTAMP);
  }

  public static ArrayIdentifierExpression ofTimestampsTz(final String name) {
    return of(name, PostgresDataType.TIMESTAMPTZ);
  }

  public static ArrayIdentifierExpression ofDates(final String name) {
    return of(name, PostgresDataType.DATE);
  }

  public static ArrayIdentifierExpression ofUuids(final String name) {
    return of(name, PostgresDataType.UUID);
  }

  public static ArrayIdentifierExpression ofBytesArray(final String name) {
    return of(name, PostgresDataType.BYTEA);
  }

  // Package-private: used internally by getPostgresArrayTypeString()
  Optional<FlatCollectionDataType> getArrayElementType() {
    return Optional.ofNullable(arrayElementType);
  }

  /**
   * Returns the PostgreSQL array type string for this expression, if the element type is specified.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>TEXT → "text[]"
   *   <li>INTEGER → "integer[]"
   *   <li>BOOLEAN → "boolean[]"
   *   <li>FLOAT → "real[]"
   *   <li>DOUBLE → "double precision[]"
   * </ul>
   *
   * @return Optional containing the PostgreSQL array type string, or empty if no type is specified
   * @throws IllegalArgumentException if the element type is not a PostgresDataType
   */
  public Optional<String> getPostgresArrayTypeString() {
    return getArrayElementType().map(this::toPostgresArrayType);
  }

  private String toPostgresArrayType(FlatCollectionDataType type) {
    if (!(type instanceof PostgresDataType)) {
      throw new IllegalArgumentException(
          "Only PostgresDataType is currently supported, got: " + type.getClass());
    }
    PostgresDataType postgresType = (PostgresDataType) type;
    String baseType = postgresType.getPgTypeName();

    // Map internal type names to SQL array type names
    switch (baseType) {
      case "int4":
        return "integer[]";
      case "int8":
        return "bigint[]";
      case "int2":
        return "smallint[]";
      case "float4":
        return "real[]";
      case "float8":
        return "double precision[]";
      case "bool":
        return "boolean[]";
      case "bytea":
        return "bytea[]";
      default:
        // For most types, just append []
        return baseType + "[]";
    }
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
