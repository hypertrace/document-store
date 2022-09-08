package org.hypertrace.core.documentstore.query;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.expression.type.SortTypeExpression;

/**
 * A generic ORDER BY definition that supports expressions. Note that this class is a more general
 * version of {@link org.hypertrace.core.documentstore.OrderBy}
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SortingSpec {

  SortTypeExpression expression;

  SortOrder order;

  public static SortingSpec of(SortTypeExpression expression, SortOrder order) {
    Preconditions.checkArgument(expression != null, "expression is null");
    Preconditions.checkArgument(order != null, "order is null");
    return new SortingSpec(expression, order);
  }

  @Override
  public String toString() {
    return expression + " " + order;
  }
}
