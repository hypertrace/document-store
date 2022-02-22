package org.hypertrace.core.documentstore.query;

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
}
