package org.hypertrace.core.documentstore.expression.operators;

public enum ArrayOperator {
  ANY,
  // Array attribute must contain every value specified in the filter
  ALL,
  // Array attribute must contain exactly one element, and that element must be one of the values
  // specified in the filter
  ONE,
}
