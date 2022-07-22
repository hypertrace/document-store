package org.hypertrace.core.documentstore.expression.operators;

public enum AggregationOperator {
  AVG,
  COUNT,
  DISTINCT, // This operator generates an array of distinct values
  DISTINCT_COUNT,
  SUM,
  MIN,
  MAX,
}
