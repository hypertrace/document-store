package org.hypertrace.core.documentstore.query;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PaginationDefinition {

  int limit;
  int offset;

  public static PaginationDefinition of(int limit) {
    return new PaginationDefinition(limit, 0);
  }

  public static PaginationDefinition of(int limit, int offset) {
    return new PaginationDefinition(limit, offset);
  }
}
