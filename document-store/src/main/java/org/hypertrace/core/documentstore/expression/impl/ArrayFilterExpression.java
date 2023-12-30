package org.hypertrace.core.documentstore.expression.impl;

import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

public interface ArrayFilterExpression {
  ArrayOperator getOperator();

  SelectTypeExpression getArraySource();

  FilterTypeExpression getFilter();
}
