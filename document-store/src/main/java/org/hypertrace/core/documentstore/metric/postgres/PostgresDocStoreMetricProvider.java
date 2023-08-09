package org.hypertrace.core.documentstore.metric.postgres;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.model.config.CustomMetricConfig.VALUE_KEY;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.metric.BaseDocStoreMetricProviderImpl;
import org.hypertrace.core.documentstore.metric.DocStoreMetric;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

@Slf4j
public class PostgresDocStoreMetricProvider extends BaseDocStoreMetricProviderImpl {
  private static final String NUM_ACTIVE_CONNECTIONS_METRIC_NAME =
      "num.active.postgres.connections";
  private static final String APP_NAME_LABEL = "app_name";
  private static final String APPLICATION_COLUMN_NAME = "application_name";
  private static final String PG_STAT_ACTIVITY_TABLE = "pg_stat_activity";

  private final String applicationNameInCurrentConnection;

  public PostgresDocStoreMetricProvider(
      final PostgresDatastore datastore, final PostgresConnectionConfig connectionConfig) {
    super(datastore);
    this.applicationNameInCurrentConnection = connectionConfig.applicationName();
  }

  @Override
  public DocStoreMetric getConnectionCountMetric() {
    final List<DocStoreMetric> metrics =
        getCustomMetrics(
            CustomMetricConfig.builder()
                .metricName(NUM_ACTIVE_CONNECTIONS_METRIC_NAME)
                .collectionName(PG_STAT_ACTIVITY_TABLE)
                .query(
                    Query.builder()
                        .addSelection(
                            SelectionSpec.of(
                                AggregateExpression.of(COUNT, ConstantExpression.of(1)), VALUE_KEY))
                        .setFilter(
                            RelationalExpression.of(
                                IdentifierExpression.of(APPLICATION_COLUMN_NAME),
                                EQ,
                                ConstantExpression.of(applicationNameInCurrentConnection)))
                        .build())
                .build());

    final DocStoreMetric defaultMetric =
        DocStoreMetric.builder()
            .name(NUM_ACTIVE_CONNECTIONS_METRIC_NAME)
            .labels(Map.of(APP_NAME_LABEL, applicationNameInCurrentConnection))
            .build();
    switch (metrics.size()) {
      case 0:
        log.error("Could not report metric {}", NUM_ACTIVE_CONNECTIONS_METRIC_NAME);
        return defaultMetric;

      case 1:
        final DocStoreMetric metric =
            metrics.get(0).toBuilder()
                .labels(Map.of(APP_NAME_LABEL, applicationNameInCurrentConnection))
                .build();
        log.debug("Returned metric: {}", metric);
        return metric;

      default:
        log.error(
            "Found {} values for metric {}", metrics.size(), NUM_ACTIVE_CONNECTIONS_METRIC_NAME);
        return defaultMetric;
    }
  }
}
