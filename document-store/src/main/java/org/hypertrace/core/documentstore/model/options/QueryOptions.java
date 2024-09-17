package org.hypertrace.core.documentstore.model.options;

import java.time.Duration;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true, chain = true)
public class QueryOptions {
  public static final QueryOptions DEFAULT_QUERY_OPTIONS = QueryOptions.builder().build();

  @Default DataFreshness dataFreshness = DataFreshness.SYSTEM_DEFAULT;
  @Default Duration queryTimeout = Duration.ofMinutes(1);
}
