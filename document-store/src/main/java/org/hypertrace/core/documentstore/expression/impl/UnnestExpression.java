package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;

/**
 * This expression allows expanding an array field to a set of rows
 *
 * <p><code>
 *  UnnestExpression.of(IdentifierExpression.of("array_col")) </code>
 */
@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class UnnestExpression implements FromTypeExpression {

  IdentifierExpression identifierExpression;
  boolean preserveNullAndEmptyArrays;
  FilterTypeExpression filterTypeExpression;

  public static UnnestExpression of(
      final IdentifierExpression identifierExpression, boolean preserveNullAndEmptyArrays) {
    Preconditions.checkArgument(identifierExpression != null, "expression is null");
    return new UnnestExpression(identifierExpression, preserveNullAndEmptyArrays, null);
  }

  public static class UnnestExpressionBuilder {
    public UnnestExpression build() {
      Preconditions.checkArgument(identifierExpression != null, "expression is null");
      return new UnnestExpression(
          identifierExpression, preserveNullAndEmptyArrays, filterTypeExpression);
    }
  }

  @Override
  public <T> T accept(FromTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
