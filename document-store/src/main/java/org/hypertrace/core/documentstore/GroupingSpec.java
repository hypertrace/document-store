package org.hypertrace.core.documentstore;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.hypertrace.core.documentstore.expression.Expression;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class GroupingSpec {
  private Expression expression; // The expression to be grouped
  private GroupBy.Accumulator accumulator; // The accumulator function
  private String alias; // Name of the accumulation result

  @Override
  public String toString() {
    return String.format("%s(%s) AS %s", accumulator.name(), expression.toString(), alias);
  }
}
