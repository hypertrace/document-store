package org.hypertrace.document.store.metric.exporter;

import java.time.Duration;
import org.hypertrace.core.documentstore.query.Query;

public interface CommonCustomMetricQueryProvider {
  String collectionName();

  String metricName();

  Query getQuery();

  default Duration reportingInterval() {
    return Duration.ofMinutes(30);
  }
}
