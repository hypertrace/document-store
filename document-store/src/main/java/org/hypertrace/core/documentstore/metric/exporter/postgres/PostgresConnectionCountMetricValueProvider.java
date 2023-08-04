package org.hypertrace.core.documentstore.metric.exporter.postgres;

import static java.util.Collections.emptyList;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.metric.exporter.DBCustomMetricValueProvider;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.postgres.PostgresClient;

@Slf4j
public class PostgresConnectionCountMetricValueProvider implements DBCustomMetricValueProvider {
  private static final String NUM_ACTIVE_CONNECTIONS_METRIC_NAME =
      "num.active.postgres.connections";

  private final PostgresClient client;
  private final String applicationNameInCurrentConnection;

  public PostgresConnectionCountMetricValueProvider(
      final PostgresConnectionConfig connectionConfig, final PostgresClient client) {
    this.client = client;
    this.applicationNameInCurrentConnection = connectionConfig.applicationName();
  }

  @Override
  public String metricName() {
    return NUM_ACTIVE_CONNECTIONS_METRIC_NAME;
  }

  @Override
  public List<Metric> getMetrics() {
    try (final PreparedStatement preparedStatement =
        client
            .getConnection()
            .prepareStatement(
                "select count(1) AS count from pg_stat_activity WHERE application_name = ?")) {
      preparedStatement.setString(1, applicationNameInCurrentConnection);
      final long count;

      try (final ResultSet resultSet = preparedStatement.executeQuery()) {
        count = resultSet.getLong("count");
      }

      return List.of(new Metric(count, Map.of("app_name", applicationNameInCurrentConnection)));
    } catch (final SQLException e) {
      log.warn("Unable to query the connection count for Postgres", e);
      return emptyList();
    }
  }

  @Override
  public Duration reportingInterval() {
    return Duration.ofDays(1);
  }
}
