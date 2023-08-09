package org.hypertrace.core.documentstore.model.config;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.query.Query;

@Value
@Builder
@Accessors(fluent = true)
public class CustomMetricConfig {
  public static String VALUE_KEY = "metric_value";

  String collectionName;
  String metricName;
  Query query;
}
