package org.hypertrace.core.documentstore.metric;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BaseMetricStoreImplTest {

  private final String collectionName = "collectionName";
  private final String metricName = "metricName";
  private final Query query = Query.builder().build();
  private final CustomMetricConfig customMetricConfig =
      CustomMetricConfig.builder()
          .collectionName(collectionName)
          .metricName(metricName)
          .query(query)
          .build();

  @Mock private Datastore mockDataStore;

  @Mock private Collection mockCollection;

  @Mock private CloseableIterator<Document> mockIterator;

  private BaseMetricStoreImpl baseMetricStoreImpl;

  @BeforeEach
  void setUp() {
    baseMetricStoreImpl =
        new BaseMetricStoreImpl(mockDataStore) {
          @Override
          public Metric getConnectionCountMetric() {
            throw new UnsupportedOperationException();
          }
        };

    when(mockDataStore.getCollection(collectionName)).thenReturn(mockCollection);
  }

  @Nested
  class GetCustomMetricsTest {

    @Test
    void queryExecutionThrowsException_returnsEmptyList() {
      when(mockCollection.aggregate(query)).thenThrow(new RuntimeException());
      final List<Metric> result = baseMetricStoreImpl.getCustomMetrics(customMetricConfig);
      assertEquals(emptyList(), result);
    }

    @Test
    void queryReturnsEmptyIterator_returnsEmptyList() {
      when(mockCollection.aggregate(query)).thenReturn(CloseableIterator.emptyIterator());
      final List<Metric> result = baseMetricStoreImpl.getCustomMetrics(customMetricConfig);
      assertEquals(emptyList(), result);
    }

    @Test
    void queryReturnsInvalidJson_returnsEmptyList() {
      when(mockCollection.aggregate(query)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next()).thenReturn(() -> "invalid-json");
      final List<Metric> result = baseMetricStoreImpl.getCustomMetrics(customMetricConfig);
      assertEquals(emptyList(), result);
    }

    @Test
    void queryReturnsDocumentWithMissingValue_returnsEmptyList() throws IOException {
      when(mockCollection.aggregate(query)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next()).thenReturn(new JSONDocument(Map.of("label1", "l1value")));
      final List<Metric> result = baseMetricStoreImpl.getCustomMetrics(customMetricConfig);
      assertEquals(emptyList(), result);
    }

    @Test
    void queryReturnsValidAndInvalidDocument_returnsEmptyList() throws IOException {
      when(mockCollection.aggregate(query)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, true, true, true, true, false);
      when(mockIterator.next())
          .thenReturn(
              () -> "invalid-json",
              new JSONDocument(Map.of("label1", "l1value", "label2", "l2value", "metric_value", 1)),
              new JSONDocument(Map.of("label1", "l1value")),
              new JSONDocument(Map.of("label1", "l2value", "metric_value", 2)),
              new JSONDocument(Map.of("label1", "l2value", "metric_value", 3)));
      final List<Metric> expected =
          List.of(
              Metric.builder()
                  .name(metricName)
                  .value(1)
                  .labels(Map.of("label1", "l1value", "label2", "l2value"))
                  .build(),
              Metric.builder()
                  .name(metricName)
                  .value(2)
                  .labels(Map.of("label1", "l2value"))
                  .build(),
              Metric.builder()
                  .name(metricName)
                  .value(3)
                  .labels(Map.of("label1", "l2value"))
                  .build());
      final List<Metric> result = baseMetricStoreImpl.getCustomMetrics(customMetricConfig);
      assertEquals(expected, result);
    }
  }
}
