package org.hypertrace.core.documentstore.metric.exporter;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
public class CommonMetricExporter implements MetricExporter {
  public static final String VALUE_KEY = "value";

  private static final ObjectMapper mapper = new ObjectMapper();
  private final Datastore dataStore;
  private final Set<CustomMetricConfig> metricConfigs;
  private final ScheduledExecutorService executorService;

  public CommonMetricExporter(final Datastore dataStore, final DatastoreConfig datastoreConfig) {
    this.metricConfigs = datastoreConfig.metricExporterConfig().customMetricConfigs();
    this.dataStore = dataStore;
    this.executorService =
        Executors.newScheduledThreadPool(
            datastoreConfig.metricExporterConfig().numMetricReporterThreads());
    log.info("Started common metrics reporter using {}", datastoreConfig.metricExporterConfig());
  }

  @Override
  public void reportMetrics() {
    for (final CustomMetricConfig metricConfig : metricConfigs) {
      executorService.scheduleAtFixedRate(
          () -> safeReport(metricConfig),
          60,
          metricConfig.reportingInterval().toSeconds(),
          SECONDS);
    }
  }

  private void safeReport(final CustomMetricConfig metricConfig) {
    try {
      report(metricConfig);
    } catch (final Exception e) {
      log.warn(
          "Unable to report custom metric {} with query \"{}\"",
          metricConfig.metricName(),
          metricConfig.query(),
          e);
    }
  }

  private void report(final CustomMetricConfig metricConfig) throws JsonProcessingException {
    final String collectionName = metricConfig.collectionName();
    final Query query = metricConfig.query();

    final Collection collection = dataStore.getCollection(collectionName);
    final CloseableIterator<Document> iterator = collection.aggregate(query);

    while (iterator.hasNext()) {
      final Document document = iterator.next();
      final JsonNode node = mapper.readTree(document.toJson());
      final long metricValue;
      if (node.has(VALUE_KEY)) {
        metricValue = Double.valueOf(node.get(VALUE_KEY).doubleValue()).longValue();
      } else {
        log.warn("No value found for {} with query {}", metricConfig.metricName(), query);
        return;
      }

      final Map<String, String> labels =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(node.fields(), Spliterator.ORDERED), false)
              .filter(entry -> !VALUE_KEY.equals(entry.getKey()))
              .collect(toUnmodifiableMap(Entry::getKey, entry -> entry.getValue().textValue()));

      PlatformMetricsRegistry.registerGauge(metricConfig.metricName(), labels, metricValue);
    }

    log.debug("Successfully reported {}", metricConfig.metricName());
  }
}
