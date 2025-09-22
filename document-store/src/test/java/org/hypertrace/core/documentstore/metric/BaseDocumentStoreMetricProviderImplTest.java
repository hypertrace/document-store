package org.hypertrace.core.documentstore.metric;

import static java.util.Collections.emptyList;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.NEAR_REALTIME_FRESHNESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BaseDocumentStoreMetricProviderImplTest {

  private final String collectionName = "collectionName";
  private final String metricName = "metricName";
  private final QueryOptions queryOptions =
      QueryOptions.builder()
          .queryTimeout(Duration.ofMinutes(20))
          .dataFreshness(NEAR_REALTIME_FRESHNESS)
          .build();
  private Query query =
      Query.builder()
          .addSelection(IdentifierExpression.of("label1"), "label1")
          .addSelection(IdentifierExpression.of("label2"), "label2")
          .build();
  private CustomMetricConfig customMetricConfig =
      CustomMetricConfig.builder()
          .collectionName(collectionName)
          .metricName(metricName)
          .query(query)
          .build();

  @Mock private Datastore mockDataStore;

  @Mock private Collection mockCollection;

  @Mock private CloseableIterator<Document> mockIterator;

  private BaseDocStoreMetricProviderImpl baseDocStoreMetricProvider;

  @BeforeEach
  void setUp() {
    baseDocStoreMetricProvider =
        new BaseDocStoreMetricProviderImpl(mockDataStore) {
          @Override
          public DocStoreMetric getConnectionCountMetric() {
            throw new UnsupportedOperationException();
          }
        };

    when(mockDataStore.getCollection(collectionName)).thenReturn(mockCollection);
  }

  @Nested
  class GetCustomMetricsTest {

    @Test
    void queryExecutionThrowsException_returnsEmptyList() {
      when(mockCollection.query(query, queryOptions)).thenThrow(new RuntimeException());
      final List<DocStoreMetric> result =
          baseDocStoreMetricProvider.getCustomMetrics(customMetricConfig);
      assertEquals(emptyList(), result);
    }

    @Test
    void queryReturnsEmptyIterator_returnsEmptyList() {
      when(mockCollection.query(query, queryOptions)).thenReturn(CloseableIterator.emptyIterator());
      final List<DocStoreMetric> result =
          baseDocStoreMetricProvider.getCustomMetrics(customMetricConfig);
      assertEquals(emptyList(), result);
    }

    @Test
    void queryReturnsInvalidJson_returnsEmptyList() {
      when(mockCollection.query(query, queryOptions)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next())
          .thenReturn(
              new Document() {
                @Override
                public String toJson() {
                  return "invalid-json";
                }

                @Override
                public DocumentType getDocumentType() {
                  return null;
                }
              });
      final List<DocStoreMetric> result =
          baseDocStoreMetricProvider.getCustomMetrics(customMetricConfig);
      assertEquals(emptyList(), result);
    }

    @Test
    void queryReturnsDocumentWithMissingValue_returnsEmptyList() throws IOException {
      when(mockCollection.query(query, queryOptions)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next()).thenReturn(JSONDocument.fromObject(Map.of("label1", "l1value")));
      final List<DocStoreMetric> result =
          baseDocStoreMetricProvider.getCustomMetrics(customMetricConfig);
      assertEquals(emptyList(), result);
    }

    @Test
    void queryReturnsValidAndInvalidDocument_returnsEmptyList() throws IOException {
      when(mockCollection.query(query, queryOptions)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, true, true, true, true, false);
      when(mockIterator.next())
          .thenReturn(
              new Document() {
                @Override
                public String toJson() {
                  return "invalid-json";
                }

                @Override
                public DocumentType getDocumentType() {
                  return null;
                }
              },
              JSONDocument.fromObject(
                  Map.of("label1", "l1value", "label2", "l2value", "metric_value", 1)),
              JSONDocument.fromObject(Map.of("label1", "l1value")),
              JSONDocument.fromObject(Map.of("label1", "l2value", "metric_value", 2)),
              JSONDocument.fromObject(Map.of("label1", "l2value", "metric_value", 3)));
      final List<DocStoreMetric> expected =
          List.of(
              DocStoreMetric.builder()
                  .name(metricName)
                  .value(1)
                  .labels(Map.of("label1", "l1value", "label2", "l2value"))
                  .build(),
              DocStoreMetric.builder()
                  .name(metricName)
                  .value(2)
                  .labels(Map.of("label1", "l2value", "label2", "<null>"))
                  .build(),
              DocStoreMetric.builder()
                  .name(metricName)
                  .value(3)
                  .labels(Map.of("label1", "l2value", "label2", "<null>"))
                  .build());
      final List<DocStoreMetric> result =
          baseDocStoreMetricProvider.getCustomMetrics(customMetricConfig);
      assertEquals(expected, result);
    }
  }
}
