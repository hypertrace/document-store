package org.hypertrace.core.documentstore.expression.operators;

public enum FunctionOperator {
  // Unary operations
  ABS,
  FLOOR,
  LENGTH,

  // n-ary operations
  ADD,
  DIVIDE, // Dividend and then divisors (Left to right associative),
  MULTIPLY,
  SUBTRACT, // Minuend and then subtrahends (Left to right associative),
}
