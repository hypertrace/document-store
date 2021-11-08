package org.hypertrace.core.documentstore.expression;

public enum ArithmeticOperator {
  // Unary operations
  ABS,
  FLOOR,

  // Binary operations
  SUBTRACT, // Minuend and then subtrahend
  DIVIDE, // Dividend and then divisor

  // n-ary operations
  ADD,
  MULTIPLY,
}
