package org.hypertrace.core.documentstore.metric;

import static java.util.Collections.emptyMap;

import java.util.Map;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.index.qual.NonNegative;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class DocStoreMetric {
  @NonNull String name;
  @Default @NonNegative double value = 0;
  @Default Map<String, String> labels = emptyMap();
}
