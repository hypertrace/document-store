package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Pagination {
  @NotNull Integer limit;
  @NotNull Integer offset;

  public static class PaginationBuilder {
    public Pagination build() {
      return validateAndReturn(new Pagination(limit, offset));
    }
  }
}
