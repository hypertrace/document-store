package org.hypertrace.core.documentstore.query;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Filter {
  FilteringExpression expression;

  public static class FilterBuilder {
    public Filter build() {
      Preconditions.checkArgument(expression != null, "expression is null");
      return new Filter(expression);
    }
  }
}
