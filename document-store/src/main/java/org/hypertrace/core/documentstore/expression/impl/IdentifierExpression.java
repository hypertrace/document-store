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
 */
@Value
@NonFinal
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class IdentifierExpression
    implements GroupTypeExpression, SelectTypeExpression, SortTypeExpression {

  String name;

  public static IdentifierExpression of(final String name) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    return new IdentifierExpression(name);
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
