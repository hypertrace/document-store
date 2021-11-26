package org.hypertrace.core.documentstore.query;

import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
class Selection {
  @Singular @NotEmpty List<@NotNull @Valid SelectionSpec> selectionSpecs;
}
