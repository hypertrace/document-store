package org.hypertrace.core.documentstore.query;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Pagination {
  Integer limit;
  Integer offset;

  public static class PaginationBuilder {
    public Pagination build() {
      Preconditions.checkArgument(limit != null, "limit is null");
      Preconditions.checkArgument(offset != null, "offset is null");

      Preconditions.checkArgument(limit >= 0, "limit must be non-negative");
      Preconditions.checkArgument(offset >= 0, "offset must be non-negative");

      return new Pagination(limit, offset);
    }
  }

  @Override
  public String toString() {
    return String.format("LIMIT %s OFFSET %s", limit, offset);
  }
}
