package org.hypertrace.document.store.metric.exporter.mongo;

import static java.util.stream.Collectors.toUnmodifiableList;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.hypertrace.document.store.metric.exporter.DBCustomMetricValueProvider;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class MongoConnectionCustomMetricQueryProvider implements DBCustomMetricValueProvider {
  private final MongoClient client;

  @Override
  public String metricName() {
    return "num.active.mongo.connections";
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Metric> getMetrics() {
    final MongoDatabase database = client.getDatabase("admin");
    final Document document =
        database.runCommand(new Document("currentOp", 1)).append("$all", true);
    final List<Map<String, Object>> processes = document.get("inprog", List.class);
    final Map<String, Long> connectionsPerApp = new HashMap<>();

    for (final Map<String, Object> process : processes) {
      final String appName = process.getOrDefault("appName", "").toString();
      connectionsPerApp.merge(appName, 1L, Math::addExact);
    }

    return connectionsPerApp.entrySet().stream()
        .map(entry -> new Metric(entry.getValue(), Map.of("app_name", entry.getKey())))
        .collect(toUnmodifiableList());
  }

  @Override
  public Duration reportingInterval() {
    return Duration.ofMinutes(1);
  }
}
