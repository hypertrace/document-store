package org.hypertrace.core.documentstore.model.config;

import static java.util.Collections.emptySet;

import java.util.Set;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class MetricExporterConfig {
  @Default boolean exportingEnabled = false;
  @Default int numMetricReporterThreads = 1;
  @NonNull @Default Set<CustomMetricConfig> customMetricConfigs = emptySet();
}
