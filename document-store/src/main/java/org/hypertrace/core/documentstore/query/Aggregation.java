package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.CacheStrategy;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode(cacheStrategy = CacheStrategy.LAZY)
public class Aggregation {
  @Singular @NotEmpty List<@NotNull GroupingExpression> expressions;

  public static class AggregationBuilder {
    public Aggregation build() {
      return validateAndReturn(new Aggregation(expressions));
    }
  }
}
