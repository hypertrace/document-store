package org.hypertrace.core.documentstore.metric.postgres;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.model.config.CustomMetricConfig.VALUE_KEY;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.metric.BaseMetricStoreImpl;
import org.hypertrace.core.documentstore.metric.Metric;
import org.hypertrace.core.documentstore.metric.MetricStore;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;

@Slf4j
public class PostgresMetricStore extends BaseMetricStoreImpl implements MetricStore {
  private static final String NUM_ACTIVE_CONNECTIONS_METRIC_NAME =
      "num.active.postgres.connections";
  private static final String APP_NAME_LABEL = "app_name";

  private final String applicationNameInCurrentConnection;

  public PostgresMetricStore(
      final Datastore datastore, final PostgresConnectionConfig connectionConfig) {
    super(datastore);
    this.applicationNameInCurrentConnection = connectionConfig.applicationName();
  }

  @Override
  public Metric getConnectionCountMetric() {
    final List<Metric> metrics =
        getCustomMetrics(
            CustomMetricConfig.builder()
                .metricName(NUM_ACTIVE_CONNECTIONS_METRIC_NAME)
                .collectionName("pg_stat_activity")
                .query(
                    Query.builder()
                        .addSelection(
                            SelectionSpec.of(
                                AggregateExpression.of(COUNT, ConstantExpression.of(1)), VALUE_KEY))
                        .setFilter(
                            RelationalExpression.of(
                                IdentifierExpression.of("application_name"),
                                EQ,
                                ConstantExpression.of(applicationNameInCurrentConnection)))
                        .build())
                .build());

    switch (metrics.size()) {
      case 0:
        log.error("Could not report metric {}", NUM_ACTIVE_CONNECTIONS_METRIC_NAME);
        return Metric.builder().name(NUM_ACTIVE_CONNECTIONS_METRIC_NAME).build();

      case 1:
        final Metric metric =
            metrics.get(0).toBuilder()
                .labels(Map.of(APP_NAME_LABEL, applicationNameInCurrentConnection))
                .build();
        log.debug("Returned metric: {}", metric);
        return metric;

      default:
        log.error(
            "Found {} values for metric {}", metrics.size(), NUM_ACTIVE_CONNECTIONS_METRIC_NAME);
        return Metric.builder().name(NUM_ACTIVE_CONNECTIONS_METRIC_NAME).build();
    }
  }
}
