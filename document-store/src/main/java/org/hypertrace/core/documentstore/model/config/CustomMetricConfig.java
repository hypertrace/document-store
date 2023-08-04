package org.hypertrace.core.documentstore.model.config;

import java.time.Duration;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.query.Query;

@Value
@Builder
@Accessors(fluent = true)
public class CustomMetricConfig {
  String collectionName;
  String metricName;
  Query query;
  Duration reportingInterval;
}
