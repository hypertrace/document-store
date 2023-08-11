package org.hypertrace.core.documentstore.metric.mongo;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.hypertrace.core.documentstore.metric.DocStoreMetric;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;
import org.hypertrace.core.documentstore.model.config.mongo.MongoDefaults;
import org.hypertrace.core.documentstore.mongo.MongoDatastore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MongoDocumentStoreMetricProviderTest {
  private final String MONGO_CLIENT_APP_NAME = "mongo-client-app-name";

  private final MongoConnectionConfig mockConnectionConfig =
      (MongoConnectionConfig)
          MongoConnectionConfig.builder().applicationName(MONGO_CLIENT_APP_NAME).build();

  @Mock private MongoDatastore mockDataStore;
  @Mock private MongoClient mockClient;
  private MongoDatabase mockDatabase;

  private MongoDocStoreMetricProvider mongoDocStoreMetricProvider;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(MongoDatabase.class, RETURNS_DEEP_STUBS);
    when(mockClient.getDatabase(MongoDefaults.ADMIN_DATABASE)).thenReturn(mockDatabase);
    mongoDocStoreMetricProvider =
        new MongoDocStoreMetricProvider(mockDataStore, mockConnectionConfig, mockClient);
  }

  @Nested
  class GetConnectionCountMetricTest {
    private final DocStoreMetric defaultMetric =
        DocStoreMetric.builder()
            .name("num.active.mongo.connections")
            .value(0L)
            .labels(Map.ofEntries(Map.entry("app_name", MONGO_CLIENT_APP_NAME)))
            .build();

    @Test
    void withEmptyProcess_returnsDefaultMetric() {
      when(mockDatabase.runCommand(new Document("currentOp", 1)).append("$all", true))
          .thenReturn(new Document("inprog", emptyList()));
      final DocStoreMetric result = mongoDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withException_returnsDefaultMetric() {
      when(mockDatabase.runCommand(new Document("currentOp", 1)).append("$all", true))
          .thenThrow(new RuntimeException());
      final DocStoreMetric result = mongoDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withNoAppName_returnsDefaultMetric() {
      when(mockDatabase.runCommand(new Document("currentOp", 1)).append("$all", true))
          .thenReturn(new Document("inprog", List.of(Map.of("active", true))));
      final DocStoreMetric result = mongoDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withNoActiveField_returnsDefaultMetric() {
      when(mockDatabase.runCommand(new Document("currentOp", 1)).append("$all", true))
          .thenReturn(new Document("inprog", List.of(Map.of("appName", MONGO_CLIENT_APP_NAME))));
      final DocStoreMetric result = mongoDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withSomeActiveConnections_returnsProperMetric() {
      when(mockDatabase.runCommand(new Document("currentOp", 1)).append("$all", true))
          .thenReturn(
              new Document(
                  "inprog",
                  List.of(
                      Map.of("active", true),
                      Map.of("appName", MONGO_CLIENT_APP_NAME),
                      Map.of("appName", MONGO_CLIENT_APP_NAME, "active", true),
                      Map.of("appName", MONGO_CLIENT_APP_NAME, "active", true),
                      Map.of("appName", MONGO_CLIENT_APP_NAME, "active", false),
                      Map.of("appName", MONGO_CLIENT_APP_NAME + 1, "active", true),
                      Map.of("appName", MONGO_CLIENT_APP_NAME, "active", true))));
      final DocStoreMetric result = mongoDocStoreMetricProvider.getConnectionCountMetric();
      final DocStoreMetric expected =
          DocStoreMetric.builder()
              .name("num.active.mongo.connections")
              .value(3)
              .labels(Map.ofEntries(Map.entry("app_name", MONGO_CLIENT_APP_NAME)))
              .build();
      assertEquals(expected, result);
    }
  }
}
