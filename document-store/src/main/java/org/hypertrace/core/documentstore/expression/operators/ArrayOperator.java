package org.hypertrace.core.documentstore.expression.operators;

public enum ArrayOperator {
  ANY,
  /**
   * Array attribute must contain every value specified in the filter. Set-containment semantics:
   * order and duplicates are irrelevant on both sides, e.g. [red, red] ALL [red] is true.
   */
  ALL,
  /**
   * Array attribute must contain exactly one element, and that element must be one of the values
   * specified in the filter. The cardinality check is on the raw element count, not distinct
   * values, e.g. [red, red] EXACTLY_ONE [red] is false because the array has two elements.
   */
  EXACTLY_ONE,
  // Future consideration: an EXACTLY operator for set equality - the array contains exactly the
  // filter values, no more and no less.
}
