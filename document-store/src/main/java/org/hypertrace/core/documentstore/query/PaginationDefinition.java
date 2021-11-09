package org.hypertrace.core.documentstore.query;

import lombok.Value;

@Value(staticConstructor = "of")
public class PaginationDefinition {
  Integer offset;
  Integer limit;
}
