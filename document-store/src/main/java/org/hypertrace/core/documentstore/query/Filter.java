package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;

import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;

@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
class Filter {
  @NotNull FilteringExpression expression;

  static class FilterBuilder {

    FilterBuilder filters(List<FilteringExpression> expressions) {
      if (CollectionUtils.isEmpty(expressions)) {
        throw new IllegalArgumentException("Cannot have an empty list of filtering expressions");
      }

      if (expressions.size() == 1) {
        expression = expressions.get(0);
      } else {
        expression = LogicalExpression.builder().operator(AND).operands(expressions).build();
      }

      return this;
    }
  }
}
