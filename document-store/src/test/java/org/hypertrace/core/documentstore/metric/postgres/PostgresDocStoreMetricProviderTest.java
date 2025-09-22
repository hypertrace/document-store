package org.hypertrace.core.documentstore.metric.postgres;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.model.config.CustomMetricConfig.VALUE_KEY;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.NEAR_REALTIME_FRESHNESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.metric.DocStoreMetric;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
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
class PostgresDocStoreMetricProviderTest {

  private final String postgresClientAppName = "PostgresClientAppName";
  private final QueryOptions queryOptions =
      QueryOptions.builder()
          .queryTimeout(Duration.ofMinutes(20))
          .dataFreshness(NEAR_REALTIME_FRESHNESS)
          .build();

  private final PostgresConnectionConfig connectionConfig =
      (PostgresConnectionConfig)
          PostgresConnectionConfig.builder().applicationName(postgresClientAppName).build();

  @Mock private PostgresDatastore mockDatastore;
  @Mock private Collection mockCollection;
  @Mock private CloseableIterator<Document> mockIterator;

  private PostgresDocStoreMetricProvider postgresDocStoreMetricProvider;

  @BeforeEach
  void setUp() {
    when(mockDatastore.getCollection(anyString())).thenReturn(mockCollection);
    postgresDocStoreMetricProvider =
        new PostgresDocStoreMetricProvider(mockDatastore, connectionConfig);
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
    private final DocStoreMetric defaultMetric =
        DocStoreMetric.builder()
            .name("num.active.postgres.connections")
            .value(0L)
            .labels(Map.ofEntries(Map.entry("app_name", postgresClientAppName)))
            .build();

    @Test
    void withEmptyIterator_returnsDefaultMetric() {
      when(mockCollection.query(query, queryOptions)).thenReturn(CloseableIterator.emptyIterator());
      final DocStoreMetric result = postgresDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withNoValueColumnIterator_returnsDefaultMetric() throws IOException {
      when(mockCollection.query(query, queryOptions)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next())
          .thenReturn(JSONDocument.fromObject(Map.of("application_name", postgresClientAppName)));
      final DocStoreMetric result = postgresDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withQueryExecutionException_returnsDefaultMetric() {
      when(mockCollection.query(query, queryOptions)).thenThrow(new RuntimeException());
      final DocStoreMetric result = postgresDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withInvalidJson_returnsDefaultMetric() {
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
      final DocStoreMetric result = postgresDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withMultipleDocuments_returnsDefaultMetric() throws IOException {
      when(mockCollection.query(query, queryOptions)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, true, false);
      when(mockIterator.next())
          .thenReturn(
              JSONDocument.fromObject(
                  Map.of("application_name", postgresClientAppName, "metric_value", 1)));
      final DocStoreMetric result = postgresDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(defaultMetric, result);
    }

    @Test
    void withMultipleInvalidDocumentsFollowedByValidDocument_returnsTheRightMetric()
        throws IOException {
      when(mockCollection.query(query, queryOptions)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, true, true, false);
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
              JSONDocument.fromObject(Map.of("value_missing", postgresClientAppName)),
              JSONDocument.fromObject(
                  Map.of("application_name", postgresClientAppName, "metric_value", 1)));
      final DocStoreMetric expected =
          DocStoreMetric.builder()
              .name("num.active.postgres.connections")
              .value(1)
              .labels(Map.of("app_name", postgresClientAppName))
              .build();
      final DocStoreMetric result = postgresDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(expected, result);
    }

    @Test
    void withValidDocument_returnsTheRightMetric() throws IOException {
      when(mockCollection.query(query, queryOptions)).thenReturn(mockIterator);
      when(mockIterator.hasNext()).thenReturn(true, false);
      when(mockIterator.next())
          .thenReturn(
              JSONDocument.fromObject(
                  Map.of("application_name", postgresClientAppName, "metric_value", 1)));
      final DocStoreMetric expected =
          DocStoreMetric.builder()
              .name("num.active.postgres.connections")
              .value(1)
              .labels(Map.of("app_name", postgresClientAppName))
              .build();
      final DocStoreMetric result = postgresDocStoreMetricProvider.getConnectionCountMetric();
      assertEquals(expected, result);
    }
  }
}
