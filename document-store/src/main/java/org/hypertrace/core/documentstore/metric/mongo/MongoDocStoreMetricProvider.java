package org.hypertrace.core.documentstore.metric.mongo;

import static org.hypertrace.core.documentstore.model.config.mongo.MongoDefaults.ADMIN_DATABASE;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.hypertrace.core.documentstore.metric.BaseDocStoreMetricProviderImpl;
import org.hypertrace.core.documentstore.metric.DocStoreMetric;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;
import org.hypertrace.core.documentstore.mongo.MongoDatastore;

@Slf4j
public class MongoDocStoreMetricProvider extends BaseDocStoreMetricProviderImpl {
  private static final String NUM_ACTIVE_CONNECTIONS_METRIC_NAME = "num.active.mongo.connections";
  private static final String APP_NAME_LABEL = "app_name";

  private final MongoDatabase adminDb;
  private final String applicationNameInCurrentConnection;

  public MongoDocStoreMetricProvider(
      final MongoDatastore dataStore,
      final MongoConnectionConfig connectionConfig,
      final MongoClient client) {
    super(dataStore);
    adminDb = client.getDatabase(ADMIN_DATABASE);
    applicationNameInCurrentConnection = connectionConfig.applicationName();
  }

  @Override
  @SuppressWarnings("unchecked")
  public DocStoreMetric getConnectionCountMetric() {
    try {
      final Document document =
          adminDb.runCommand(new Document("currentOp", 1)).append("$all", true);
      final List<Map<String, Object>> processes = document.get("inprog", List.class);
      long count = 0;

      for (final Map<String, Object> process : processes) {
        final String appName = process.getOrDefault("appName", "").toString();
        final boolean isActive =
            Boolean.parseBoolean(process.getOrDefault("active", "false").toString());

        if (appName.equals(applicationNameInCurrentConnection) && isActive) {
          count++;
        }
      }

      final DocStoreMetric metric =
          DocStoreMetric.builder()
              .name(NUM_ACTIVE_CONNECTIONS_METRIC_NAME)
              .value(count)
              .labels(Map.of(APP_NAME_LABEL, applicationNameInCurrentConnection))
              .build();
      log.debug("Returned metric: {}", metric);
      return metric;
    } catch (final Exception e) {
      log.error("Unable to capture {}", NUM_ACTIVE_CONNECTIONS_METRIC_NAME, e);
      return DocStoreMetric.builder()
          .name(NUM_ACTIVE_CONNECTIONS_METRIC_NAME)
          .labels(Map.of(APP_NAME_LABEL, applicationNameInCurrentConnection))
          .build();
    }
  }
}
