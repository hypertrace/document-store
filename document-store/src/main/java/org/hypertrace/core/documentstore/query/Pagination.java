package org.hypertrace.core.documentstore.query;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Pagination {
  @NotNull Integer limit;
  @NotNull Integer offset;
}
