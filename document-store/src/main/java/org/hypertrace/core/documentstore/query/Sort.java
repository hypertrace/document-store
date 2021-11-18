package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Sort {
  @NotEmpty @Singular List<@NotNull SortingSpec> sortingSpecs;

  public static class SortBuilder {
    public Sort build() {
      return validateAndReturn(new Sort(sortingSpecs));
    }
  }
}
