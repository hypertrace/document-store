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
  NOT_IN("NOT IN"),
  CONTAINS("CONTAINS"),
  NOT_CONTAINS("NOT CONTAINS"),
  EXISTS("EXISTS"),
  NOT_EXISTS("NOT EXISTS"),
  LIKE("~"),
  STARTS_WITH("STARTS WITH"),
  ;

  private final String displaySymbol;

  @Override
  public String toString() {
    return displaySymbol;
  }
}
