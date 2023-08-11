package org.hypertrace.core.documentstore.metric;

import java.util.List;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;

public interface DocStoreMetricProvider {
  DocStoreMetric getConnectionCountMetric();

  List<DocStoreMetric> getCustomMetrics(final CustomMetricConfig customMetricConfig);
}
