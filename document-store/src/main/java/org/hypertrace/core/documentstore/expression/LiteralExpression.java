package org.hypertrace.core.documentstore.expression;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class LiteralExpression implements Expression {
  private String literal;

  @Override
  public String toString() {
    return literal;
  }
}
