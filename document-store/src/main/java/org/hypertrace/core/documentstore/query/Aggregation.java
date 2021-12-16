package org.hypertrace.core.documentstore.query;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Aggregation {
  @Singular List<GroupingExpression> expressions;

  public static class AggregationBuilder {
    public Aggregation build() {
      Preconditions.checkArgument(!expressions.isEmpty(), "expressions is empty");
      Preconditions.checkArgument(
          expressions.stream().noneMatch(Objects::isNull), "One ore more expressions is null");
      return new Aggregation(expressions);
    }
  }
}
