package org.hypertrace.core.documentstore.metric.exporter.mongo;

import static org.hypertrace.core.documentstore.model.config.mongo.MongoDefaults.ADMIN_DATABASE;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.hypertrace.core.documentstore.metric.exporter.DBCustomMetricValueProvider;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;

@Slf4j
public class MongoConnectionCountMetricValueProvider implements DBCustomMetricValueProvider {
  private static final String NUM_ACTIVE_CONNECTIONS_METRIC_NAME = "num.active.mongo.connections";

  private final MongoDatabase database;
  private final String applicationNameInCurrentConnection;

  public MongoConnectionCountMetricValueProvider(
      final MongoConnectionConfig connectionConfig, final MongoClient client) {
    database = client.getDatabase(ADMIN_DATABASE);
    applicationNameInCurrentConnection = connectionConfig.applicationName();
  }

  @Override
  public String metricName() {
    return NUM_ACTIVE_CONNECTIONS_METRIC_NAME;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Metric> getMetrics() {
    final Document document =
        database.runCommand(new Document("currentOp", 1)).append("$all", true);
    final List<Map<String, Object>> processes = document.get("inprog", List.class);
    long count = 0;

    for (final Map<String, Object> process : processes) {
      final String appName = process.getOrDefault("appName", "").toString();
      if (appName.equals(applicationNameInCurrentConnection)) {
        count++;
      }
    }

    return List.of(new Metric(count, Map.of("app_name", applicationNameInCurrentConnection)));
  }

  @Override
  public Duration reportingInterval() {
    return Duration.ofDays(1);
  }
}
