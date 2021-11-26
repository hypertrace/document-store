package org.hypertrace.core.documentstore.query;

import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;

@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
class Aggregation {
  @Singular @NotEmpty List<@NotNull GroupingExpression> expressions;
}
