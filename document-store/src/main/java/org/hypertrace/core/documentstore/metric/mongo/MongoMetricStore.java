package org.hypertrace.core.documentstore.metric.mongo;

import static org.hypertrace.core.documentstore.model.config.mongo.MongoDefaults.ADMIN_DATABASE;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.metric.BaseMetricStoreImpl;
import org.hypertrace.core.documentstore.metric.Metric;
import org.hypertrace.core.documentstore.metric.MetricStore;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;

@Slf4j
public class MongoMetricStore extends BaseMetricStoreImpl implements MetricStore {
  private static final String NUM_ACTIVE_CONNECTIONS_METRIC_NAME = "num.active.mongo.connections";
  private static final String APP_NAME_LABEL = "app_name";

  private final MongoDatabase adminDb;
  private final String applicationNameInCurrentConnection;

  public MongoMetricStore(
      final Datastore dataStore,
      final MongoConnectionConfig connectionConfig,
      final MongoClient client) {
    super(dataStore);
    adminDb = client.getDatabase(ADMIN_DATABASE);
    applicationNameInCurrentConnection = connectionConfig.applicationName();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Metric getConnectionCountMetric() {
    try {
      final Document document =
          adminDb.runCommand(new Document("currentOp", 1)).append("$all", true);
      final List<Map<String, Object>> processes = document.get("inprog", List.class);
      long count = 0;

      for (final Map<String, Object> process : processes) {
        final String appName = process.getOrDefault("appName", "").toString();
        if (appName.equals(applicationNameInCurrentConnection)) {
          count++;
        }
      }

      final Metric metric =
          Metric.builder()
              .name(NUM_ACTIVE_CONNECTIONS_METRIC_NAME)
              .value(count)
              .labels(Map.of(APP_NAME_LABEL, applicationNameInCurrentConnection))
              .build();
      log.debug("Returned metric: {}", metric);
      return metric;
    } catch (final Exception e) {
      log.error("Unable to capture {}", NUM_ACTIVE_CONNECTIONS_METRIC_NAME, e);
      return Metric.builder().name(NUM_ACTIVE_CONNECTIONS_METRIC_NAME).build();
    }
  }
}
