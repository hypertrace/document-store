package org.hypertrace.core.documentstore.model.options;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true, chain = true)
public class QueryOptions {
  public static final QueryOptions DEFAULT_QUERY_OPTIONS = QueryOptions.builder().build();

  @Default DataFreshness dataFreshness = DataFreshness.REAL_TIME_FRESHNESS;
}
