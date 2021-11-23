package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.CacheStrategy;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode(cacheStrategy = CacheStrategy.LAZY)
public class Filter {
  @NotNull FilteringExpression expression;

  public static class FilterBuilder {
    public Filter build() {
      return validateAndReturn(new Filter(expression));
    }
  }
}
