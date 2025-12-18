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
 * <p>For flat relational collections, you can optionally provide a {@link DataType} to enable
 * type-safe query generation without runtime type inference:
 *
 * <p>Example: IdentifierExpression.ofInt("price");
 */
@Value
@NonFinal
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class IdentifierExpression
    implements GroupTypeExpression, SelectTypeExpression, SortTypeExpression {

  String name;
  // Type information of this identifier for flat collections, this is optional to maintain backward
  // compatibility
  DataType dataType;

  IdentifierExpression(String name) {
    this.name = name;
    this.dataType = DataType.UNSPECIFIED;
  }

  public static IdentifierExpression of(final String name) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    return new IdentifierExpression(name);
  }

  static IdentifierExpression of(final String name, final DataType dataType) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    return new IdentifierExpression(name, dataType);
  }

  public static IdentifierExpression ofString(final String name) {
    return of(name, DataType.STRING);
  }

  public static IdentifierExpression ofInt(final String name) {
    return of(name, DataType.INTEGER);
  }

  public static IdentifierExpression ofLong(final String name) {
    return of(name, DataType.LONG);
  }

  public static IdentifierExpression ofFloat(final String name) {
    return of(name, DataType.FLOAT);
  }

  public static IdentifierExpression ofDouble(final String name) {
    return of(name, DataType.DOUBLE);
  }

  public static IdentifierExpression ofBoolean(final String name) {
    return of(name, DataType.BOOLEAN);
  }

  // Timestamp with time-zone information. For example: 2004-10-19 10:23:54+02. For more info, see:
  // https://www.postgresql.org/docs/current/datatype-datetime.html
  public static IdentifierExpression ofTimestampTz(final String name) {
    return of(name, DataType.TIMESTAMPTZ);
  }

  public static IdentifierExpression ofDate(final String name) {
    return of(name, DataType.DATE);
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
