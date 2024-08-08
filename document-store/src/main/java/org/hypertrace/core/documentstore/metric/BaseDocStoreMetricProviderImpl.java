package org.hypertrace.core.documentstore.metric;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.hypertrace.core.documentstore.model.config.CustomMetricConfig.VALUE_KEY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

@Slf4j
@AllArgsConstructor
public abstract class BaseDocStoreMetricProviderImpl implements DocStoreMetricProvider {
  private static final String NULL_VALUE_PLACEHOLDER = "<null>";
  private static final ObjectMapper mapper = new ObjectMapper();

  private final Datastore dataStore;

  @Override
  public List<DocStoreMetric> getCustomMetrics(final CustomMetricConfig customMetricConfig) {
    try {
      final String collectionName = customMetricConfig.collectionName();
      final Query query = customMetricConfig.query();
      query
          .getSelections()
          .forEach(
              selectionSpec -> {
                final String alias = selectionSpec.getAlias();
                if (Objects.isNull(alias) || alias.isEmpty()) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Selection alias is required for selection spec in custom metric query for metric: %s",
                          customMetricConfig.metricName()));
                }
              });

      final Collection collection = dataStore.getCollection(collectionName);
      final CloseableIterator<Document> iterator = collection.aggregate(query);

      final List<DocStoreMetric> metrics = new ArrayList<>();

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

        final double metricValue;
        if (node.has(VALUE_KEY)) {
          metricValue = node.get(VALUE_KEY).doubleValue();
        } else {
          log.warn(
              "No value found in JSON document {} for metric {} with query {}",
              document.toJson(),
              customMetricConfig.metricName(),
              query);
          continue;
        }

        Map<String, JsonNode> jsonNodeMap =
            query.getSelections().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        SelectionSpec::getAlias,
                        selectionSpec ->
                            Optional.ofNullable(node.get(selectionSpec.getAlias()))
                                .orElse(TextNode.valueOf(NULL_VALUE_PLACEHOLDER))));

        final Map<String, String> labels =
            StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(
                        jsonNodeMap.entrySet().iterator(), Spliterator.ORDERED),
                    false)
                .filter(entry -> !VALUE_KEY.equals(entry.getKey()))
                .collect(toUnmodifiableMap(Entry::getKey, entry -> entry.getValue().textValue()));

        final DocStoreMetric metric =
            DocStoreMetric.builder()
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
