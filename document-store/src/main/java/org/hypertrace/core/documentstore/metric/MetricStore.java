package org.hypertrace.core.documentstore.metric;

import java.util.List;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;

public interface MetricStore {
  Metric getConnectionCountMetric();

  List<Metric> getCustomMetrics(final CustomMetricConfig customMetricConfig);
}
