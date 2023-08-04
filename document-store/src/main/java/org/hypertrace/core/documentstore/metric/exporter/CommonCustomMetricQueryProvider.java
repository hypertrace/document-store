package org.hypertrace.core.documentstore.metric.exporter;

import java.time.Duration;
import org.hypertrace.core.documentstore.query.Query;

public interface CommonCustomMetricQueryProvider {
  String collectionName();

  String metricName();

  Query getQuery();

  default Duration reportingInterval() {
    return Duration.ofHours(1);
  }
}
