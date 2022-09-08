package org.hypertrace.core.documentstore.query;

import static java.util.stream.Collectors.joining;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FromClause {
  @Singular List<FromTypeExpression> fromTypeExpressions;

  public static class FromClauseBuilder {
    public FromClause build() {
      Preconditions.checkArgument(!fromTypeExpressions.isEmpty(), "expressions is empty");
      Preconditions.checkArgument(
          fromTypeExpressions.stream().noneMatch(Objects::isNull),
          "One or more expressions is null");
      return new FromClause(fromTypeExpressions);
    }
  }

  @Override
  public String toString() {
    return fromTypeExpressions.stream().map(String::valueOf).collect(joining(", "));
  }
}
