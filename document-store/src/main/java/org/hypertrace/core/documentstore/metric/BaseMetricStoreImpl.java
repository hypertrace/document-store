package org.hypertrace.core.documentstore.metric;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.hypertrace.core.documentstore.model.config.CustomMetricConfig.VALUE_KEY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.query.Query;

@Slf4j
@AllArgsConstructor
public abstract class BaseMetricStoreImpl implements MetricStore {
  private static final ObjectMapper mapper = new ObjectMapper();

  private final Datastore dataStore;

  @Override
  public List<Metric> getCustomMetrics(final CustomMetricConfig customMetricConfig) {
    try {
      final String collectionName = customMetricConfig.collectionName();
      final Query query = customMetricConfig.query();

      final Collection collection = dataStore.getCollection(collectionName);
      final CloseableIterator<Document> iterator = collection.aggregate(query);

      final List<Metric> metrics = new ArrayList<>();

      while (iterator.hasNext()) {
        final Document document = iterator.next();
        final JsonNode node;

        try {
          node = mapper.readTree(document.toJson());
        } catch (final JsonProcessingException e) {
          log.warn(
              "Invalid JSON document {} for metric {} with query {}",
              document.toJson(),
              customMetricConfig.metricName(),
              query);
          continue;
        }

        final long metricValue;
        if (node.has(VALUE_KEY)) {
          metricValue = Double.valueOf(node.get(VALUE_KEY).doubleValue()).longValue();
        } else {
          log.warn(
              "No value found in JSON document {} for metric {} with query {}",
              document.toJson(),
              customMetricConfig.metricName(),
              query);
          continue;
        }

        final Map<String, String> labels =
            StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(node.fields(), Spliterator.ORDERED), false)
                .filter(entry -> !VALUE_KEY.equals(entry.getKey()))
                .collect(toUnmodifiableMap(Entry::getKey, entry -> entry.getValue().textValue()));

        final Metric metric =
            Metric.builder()
                .name(customMetricConfig.metricName())
                .value(metricValue)
                .labels(labels)
                .build();
        metrics.add(metric);
      }

      log.debug("Returning metrics: {}", metrics);
      return metrics;
    } catch (final Exception e) {
      log.error("Unable to report metric: {}", customMetricConfig, e);
      return emptyList();
    }
  }
}
