package org.hypertrace.core.documentstore.expression.operators;

public enum AggregationOperator {
  AVG,
  COUNT,
  /** @deprecated Use {@link AggregationOperator#DISTINCT_ARRAY} instead. */
  @Deprecated(forRemoval = true)
  DISTINCT,
  DISTINCT_ARRAY, // This operator generates an array of distinct values
  DISTINCT_COUNT,
  SUM,
  MIN,
  MAX,
}
