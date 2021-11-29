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
public class Selection {
  @Singular @NotEmpty List<@NotNull SelectionSpec> selectionSpecs;

  public static class SelectionBuilder {
    public Selection build() {
      return validateAndReturn(new Selection(selectionSpecs));
    }
  }
}
