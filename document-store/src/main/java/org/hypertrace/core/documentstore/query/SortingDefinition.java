package org.hypertrace.core.documentstore.query;

import lombok.Value;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;

/**
 * A generic ORDER BY definition that supports expressions. Note that this class is a more general
 * version of {@link org.hypertrace.core.documentstore.OrderBy}
 */
@Value(staticConstructor = "of")
public class SortingDefinition {
  SortingExpression expression;
  SortingOrder order;
}
