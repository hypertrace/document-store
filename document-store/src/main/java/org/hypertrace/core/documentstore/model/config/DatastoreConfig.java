package org.hypertrace.core.documentstore.model.config;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class DatastoreConfig {
  @NonNull DatabaseType type;
  @NonNull ConnectionConfig connectionConfig;

  @NonNull @Default
  MetricExporterConfig metricExporterConfig = MetricExporterConfig.builder().build();
}
