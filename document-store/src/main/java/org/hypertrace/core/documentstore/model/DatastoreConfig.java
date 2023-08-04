package org.hypertrace.core.documentstore.model;

import static java.util.Collections.emptySet;

import java.util.Set;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.metric.exporter.CommonCustomMetricQueryProvider;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.DatabaseType;

@Value
@Builder(toBuilder = true)
@Accessors(fluent = true)
public class DatastoreConfig {
  @NonNull DatabaseType type;
  @NonNull ConnectionConfig connectionConfig;
  @Builder.Default int numMetricReporterThreads = 1;
  @NonNull @Default Set<CommonCustomMetricQueryProvider> metricReporters = emptySet();
}
