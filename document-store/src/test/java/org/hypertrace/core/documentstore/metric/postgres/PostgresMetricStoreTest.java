package org.hypertrace.core.documentstore.metric.postgres;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.model.config.CustomMetricConfig.VALUE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.metric.Metric;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostgresMetricStoreTest {

  private final String postgresClientAppName = "PostgresClientAppName";
  private final PostgresConnectionConfig connectionConfig =
      (PostgresConnectionConfig)
          PostgresConnectionConfig.builder().applicationName(postgresClientAppName).build();

  @Mock private PostgresDatastore mockDatastore;
  @Mock private Collection mockCollection;
  @Mock private CloseableIterator<Document> mockIterator;

  private PostgresMetricStore postgresMetricStore;

  @BeforeEach
  void setUp() {
    when(mockDatastore.getCollection(anyString())).thenReturn(mockCollection);
    postgresMetricStore = new PostgresMetricStore(mockDatastore, connectionConfig);
  }

  @Nested
  class GetConnectionCountMetricTest {

    private final Query query =
        Query.builder()
            .addSelection(
                SelectionSpec.of(
                    AggregateExpression.of(COUNT, ConstantExpression.of(1)), VALUE_KEY))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("application_name"),
                    EQ,
                    ConstantExpression.of(postgresClientAppName)))
            .build();
    private final Metric defaultMetric =
        Metric.builder()
            .name("num.active.postgres.connections")
            .value(0L)
            .labels(Map.ofEntries(Map.entry("app_name", postgresClientAppName)))
            .build();

    @Test
    void withEmptyIterator_returnsDefaultMetric() {
      when(mockCollection.aggregate(query)).thenReturn(CloseableIterator.emptyIterator());
      final Metric result = postgresMetricStore.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withNoValueColumnIterator_returnsDefaultMetric() throws IOException {
      when(mockCollection.aggregate(query)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next())
          .thenReturn(new JSONDocument(Map.of("application_name", postgresClientAppName)));
      final Metric result = postgresMetricStore.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withQueryExecutionException_returnsDefaultMetric() {
      when(mockCollection.aggregate(query)).thenThrow(new RuntimeException());
      final Metric result = postgresMetricStore.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withInvalidJson_returnsDefaultMetric() {
      when(mockCollection.aggregate(query)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next()).thenReturn(() -> "invalid-json");
      final Metric result = postgresMetricStore.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withMultipleDocuments_returnsDefaultMetric() throws IOException {
      when(mockCollection.aggregate(query)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, true, false);
      when(mockIterator.next())
          .thenReturn(
              new JSONDocument(
                  Map.of("application_name", postgresClientAppName, "metric_value", 1)));
      final Metric result = postgresMetricStore.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withMultipleInvalidDocumentsFollowedByValidDocument_returnsTheRightMetric()
        throws IOException {
      when(mockCollection.aggregate(query)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, true, true, false);
      when(mockIterator.next())
          .thenReturn(
              () -> "invalid-json",
              new JSONDocument(Map.of("value_missing", postgresClientAppName)),
              new JSONDocument(
                  Map.of("application_name", postgresClientAppName, "metric_value", 1)));
      final Metric expected =
          Metric.builder()
              .name("num.active.postgres.connections")
              .value(1)
              .labels(Map.of("app_name", postgresClientAppName))
              .build();
      final Metric result = postgresMetricStore.getConnectionCountMetric();
      assertEquals(expected, result);
    }

    @Test
    void withValidDocument_returnsTheRightMetric() throws IOException {
      when(mockCollection.aggregate(query)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next())
          .thenReturn(
              new JSONDocument(
                  Map.of("application_name", postgresClientAppName, "metric_value", 1)));
      final Metric expected =
          Metric.builder()
              .name("num.active.postgres.connections")
              .value(1)
              .labels(Map.of("app_name", postgresClientAppName))
              .build();
      final Metric result = postgresMetricStore.getConnectionCountMetric();
      assertEquals(expected, result);
    }
  }
}
