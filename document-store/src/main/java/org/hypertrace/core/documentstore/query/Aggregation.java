package org.hypertrace.core.documentstore.query;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Aggregation {
  @Singular List<GroupTypeExpression> expressions;

  public static class AggregationBuilder {
    public Aggregation build() {
      Preconditions.checkArgument(!expressions.isEmpty(), "expressions is empty");
      Preconditions.checkArgument(
          expressions.stream().noneMatch(Objects::isNull), "One or more expressions is null");
      return new Aggregation(expressions);
    }
  }
}
