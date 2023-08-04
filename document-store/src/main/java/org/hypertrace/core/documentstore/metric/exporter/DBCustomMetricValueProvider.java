package org.hypertrace.core.documentstore.metric.exporter;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.Value;

public interface DBCustomMetricValueProvider {
  String metricName();

  List<Metric> getMetrics();

  default Duration reportingInterval() {
    return Duration.ofMinutes(30);
  }

  @Value
  class Metric {
    long value;
    Map<String, String> labels;
  }
}
