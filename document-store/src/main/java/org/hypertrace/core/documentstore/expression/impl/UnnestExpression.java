package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class UnnestExpression implements FromTypeExpression {

  IdentifierExpression identifierExpression;

  public static UnnestExpression of(final IdentifierExpression identifierExpression) {
    Preconditions.checkArgument(identifierExpression != null, "expression is null");
    return new UnnestExpression(identifierExpression);
  }

  @Override
  public <T> T accept(FromTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }
}
