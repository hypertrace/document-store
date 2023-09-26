package org.hypertrace.core.documentstore.expression.impl;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

/**
 * Expression representing the root document.
 *
 * <p>Example: <code>
 *    RootExpression.INSTANCE
 *  </code>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RootExpression implements SelectTypeExpression {
  public static final RootExpression INSTANCE = new RootExpression();

  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "<ROOT>";
  }
}
