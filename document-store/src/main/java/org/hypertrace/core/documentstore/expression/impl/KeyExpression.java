package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.commons.DocStoreConstants.IMPLICIT_ID;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class KeyExpression implements FilterTypeExpression {
  Key key;

  public static KeyExpression of(final Key key) {
    Preconditions.checkArgument(key != null, "key is null");
    return new KeyExpression(key);
  }

  @Override
  public <T> T accept(final FilterTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return IMPLICIT_ID + " = " + StringUtils.wrap(key.toString(), "'");
  }
}
