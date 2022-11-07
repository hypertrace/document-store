package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

/**
 * Expression representing either an identifier/column name of a sub-query
 *
 * <p>Example: SubQueryIdentifierExpression.of("col1");
 */
@Value
@EqualsAndHashCode(callSuper = true)
public class SubQueryIdentifierExpression extends IdentifierExpression {

  public SubQueryIdentifierExpression(final String name) {
    super(name);
  }

  public static SubQueryIdentifierExpression of(final String name) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    return new SubQueryIdentifierExpression(name);
  }

  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "`." + name + "`";
  }
}
