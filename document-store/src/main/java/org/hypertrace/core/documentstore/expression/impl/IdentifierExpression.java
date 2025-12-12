package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SortTypeExpression;
import org.hypertrace.core.documentstore.parser.FieldTransformationVisitor;
import org.hypertrace.core.documentstore.parser.GroupTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;

/**
 * Expression representing either an identifier/column name
 *
 * <p>Example: IdentifierExpression.of("col1");
 *
 * <p>For flat relational collections, you can optionally provide a {@link FlatCollectionDataType}
 * to enable type-safe query generation without runtime type inference:
 *
 * <p>Example: IdentifierExpression.of("price", PostgresType.INTEGER);
 */
@Value
@NonFinal
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class IdentifierExpression
    implements GroupTypeExpression, SelectTypeExpression, SortTypeExpression {

  String name;
  // Type information of this identifier for flat collections, optional
  FlatCollectionDataType flatCollectionDataType;

  public static IdentifierExpression of(final String name) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    return new IdentifierExpression(name, null);
  }

  // Package-private: used internally by factory methods
  static IdentifierExpression of(
      final String name, final FlatCollectionDataType flatCollectionDataType) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    return new IdentifierExpression(name, flatCollectionDataType);
  }

  public static IdentifierExpression ofString(final String name) {
    return of(name, PostgresDataType.TEXT);
  }

  public static IdentifierExpression ofInt(final String name) {
    return of(name, PostgresDataType.INTEGER);
  }

  public static IdentifierExpression ofLong(final String name) {
    return of(name, PostgresDataType.BIGINT);
  }

  public static IdentifierExpression ofShort(final String name) {
    return of(name, PostgresDataType.SMALLINT);
  }

  public static IdentifierExpression ofFloat(final String name) {
    return of(name, PostgresDataType.FLOAT);
  }

  public static IdentifierExpression ofDouble(final String name) {
    return of(name, PostgresDataType.DOUBLE);
  }

  public static IdentifierExpression ofDecimal(final String name) {
    return of(name, PostgresDataType.NUMERIC);
  }

  public static IdentifierExpression ofBoolean(final String name) {
    return of(name, PostgresDataType.BOOLEAN);
  }

  public static IdentifierExpression ofTimestamp(final String name) {
    return of(name, PostgresDataType.TIMESTAMP);
  }

  public static IdentifierExpression ofTimestampTz(final String name) {
    return of(name, PostgresDataType.TIMESTAMPTZ);
  }

  public static IdentifierExpression ofDate(final String name) {
    return of(name, PostgresDataType.DATE);
  }

  public static IdentifierExpression ofUuid(final String name) {
    return of(name, PostgresDataType.UUID);
  }

  public static IdentifierExpression ofJsonb(final String name) {
    return of(name, PostgresDataType.JSONB);
  }

  public static IdentifierExpression ofBytes(final String name) {
    return of(name, PostgresDataType.BYTEA);
  }

  @Override
  public <T> T accept(final GroupTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public <T> T accept(final SortTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  /**
   * Accepts a field transformation visitor for database-specific field transformations.
   *
   * @param visitor The field transformation visitor
   * @param <T> The return type of the transformation
   * @return The transformed field representation
   */
  public <T> T accept(final FieldTransformationVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "`" + name + "`";
  }
}
