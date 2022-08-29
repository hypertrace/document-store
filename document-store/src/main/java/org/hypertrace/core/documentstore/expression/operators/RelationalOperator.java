package org.hypertrace.core.documentstore.expression.operators;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum RelationalOperator {
  EQ("="),
  NEQ("!="),
  GT(">"),
  LT("<"),
  GTE(">="),
  LTE("<="),
  IN("IN"),
  CONTAINS("CONTAINS"),
  EXISTS("EXISTS"),
  NOT_EXISTS("NOT_EXISTS"),
  LIKE("~"),
  NOT_IN("NOT_IN"),
  ;

  private final String displaySymbol;

  @Override
  public String toString() {
    return displaySymbol;
  }
}
