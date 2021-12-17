package org.hypertrace.core.documentstore.query;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;

/**
 * A generic ORDER BY definition that supports expressions. Note that this class is a more general
 * version of {@link org.hypertrace.core.documentstore.OrderBy}
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SortingSpec {

  SortingExpression expression;

  SortingOrder order;

  public static SortingSpec of(SortingExpression expression, SortingOrder order) {
    Preconditions.checkArgument(expression != null, "expression is null");
    Preconditions.checkArgument(order != null, "order is null");
    return new SortingSpec(expression, order);
  }
}
