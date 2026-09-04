package org.hypertrace.core.documentstore.expression.operators;

public enum ArrayOperator {
  ANY,
  /**
   * With an inner {@code IN} filter, {@code IN} is element membership (not array equality). {@code
   * ALL} means the RHS set is a subset of the stored array: every listed value appears in the
   * array. Order and duplicates are ignored (set semantics), e.g. {@code [red, red] ALL [red]} is
   * true.
   */
  ALL,
  /**
   * With an inner {@code IN} filter, {@code IN} is element membership (not array equality). {@code
   * EXACTLY_ONE} means the stored array has length 1 <em>and</em> that single element is in the
   * RHS set. Order and duplicates in the RHS are ignored (set semantics). The cardinality check is
   * on the raw element count, not distinct values, e.g. {@code [red, red] EXACTLY_ONE [red]} is
   * false because the array has two elements.
   */
  EXACTLY_ONE,
  // Future consideration: an EXACTLY operator for set equality - the array contains exactly the
  // filter values, no more and no less.
}
