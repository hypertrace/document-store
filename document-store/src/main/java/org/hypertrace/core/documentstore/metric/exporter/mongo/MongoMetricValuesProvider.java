package org.hypertrace.core.documentstore.metric.exporter.mongo;

import static org.hypertrace.core.documentstore.model.config.mongo.MongoDefaults.ADMIN_DATABASE;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.hypertrace.core.documentstore.metric.exporter.DBCustomMetricValuesProvider;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;

@Slf4j
public class MongoMetricValuesProvider implements DBCustomMetricValuesProvider {
  private static final String NUM_ACTIVE_CONNECTIONS_METRIC_NAME = "num.active.mongo.connections";
  private static final String APP_NAME_LABEL = "app_name";

  private final MongoDatabase database;
  private final String applicationNameInCurrentConnection;

  public MongoMetricValuesProvider(
      final MongoConnectionConfig connectionConfig, final MongoClient client) {
    database = client.getDatabase(ADMIN_DATABASE);
    applicationNameInCurrentConnection = connectionConfig.applicationName();
    log.info("Started MongoDB metrics reporter");
  }

  @Override
  public String metricName() {
    return NUM_ACTIVE_CONNECTIONS_METRIC_NAME;
  }

  @Override
  public List<Metric> getMetrics() {
    final Metric connectionCountMetric = getConnectionCountMetric();
    return List.of(connectionCountMetric);
  }

  @Override
  public Duration reportingInterval() {
    return Duration.ofDays(1);
  }

  @SuppressWarnings("unchecked")
  private Metric getConnectionCountMetric() {
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

    return new Metric(count, Map.of(APP_NAME_LABEL, applicationNameInCurrentConnection));
  }
}
