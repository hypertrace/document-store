package org.hypertrace.core.documentstore.metric.exporter;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.metric.exporter.DBCustomMetricValueProvider.Metric;
import org.hypertrace.core.documentstore.model.DatastoreConfig;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class DBMetricExporter implements MetricExporter {
  private final Set<DBCustomMetricValueProvider> reporters;
  private final ScheduledExecutorService executorService;

  @Inject
  public DBMetricExporter(
      final Set<DBCustomMetricValueProvider> reporters, final DatastoreConfig datastoreConfig) {
    this.reporters = reporters;
    executorService = Executors.newScheduledThreadPool(datastoreConfig.numMetricReporterThreads());
  }

  @Override
  public void reportMetrics() {
    for (final DBCustomMetricValueProvider reporter : reporters) {
      executorService.scheduleAtFixedRate(
          () -> safeReport(reporter), 60, reporter.reportingInterval().toSeconds(), SECONDS);
    }
  }

  private void safeReport(final DBCustomMetricValueProvider reporter) {
    try {
      report(reporter);
    } catch (final Exception e) {
      log.warn("Unable to report custom metric {}", reporter.metricName(), e);
    }
  }

  private void report(final DBCustomMetricValueProvider reporter) {
    final String metricName = reporter.metricName();
    final List<Metric> metrics = reporter.getMetrics();

    for (final Metric metric : metrics) {
      PlatformMetricsRegistry.registerGauge(metricName, metric.getLabels(), metric.getValue());
    }

    log.debug("Successfully reported {}", reporter.metricName());
  }
}
