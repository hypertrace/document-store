package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotNull;
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
public class SortingDefinition {

  @NotNull SortingExpression expression;

  @NotNull SortingOrder order;

  public static SortingDefinition of(SortingExpression expression, SortingOrder order) {
    return validateAndReturn(new SortingDefinition(expression, order));
  }
}
