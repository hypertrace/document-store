package org.hypertrace.core.documentstore.metric.exporter.postgres;

import static java.util.Collections.emptyList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.metric.exporter.DBCustomMetricValuesProvider;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.postgres.PostgresClient;

@Slf4j
public class PostgresMetricValuesProvider implements DBCustomMetricValuesProvider {
  private static final String NUM_ACTIVE_CONNECTIONS_METRIC_NAME =
      "num.active.postgres.connections";
  private static final String APP_NAME_LABEL = "app_name";

  private final PostgresClient client;
  private final String applicationNameInCurrentConnection;

  public PostgresMetricValuesProvider(
      final PostgresConnectionConfig connectionConfig, final PostgresClient client) {
    this.client = client;
    this.applicationNameInCurrentConnection = connectionConfig.applicationName();
    log.info("Started Postgres metrics reporter");
  }

  @Override
  public String metricName() {
    return NUM_ACTIVE_CONNECTIONS_METRIC_NAME;
  }

  @Override
  public List<Metric> getMetrics() {
    final Connection connection = client.getConnection();
    try {
      final Metric connectionCountMetric = getConnectionCountMetric(connection);
      return List.of(connectionCountMetric);
    } catch (final SQLException e) {
      log.warn("Unable to query the connection count for Postgres", e);
      return emptyList();
    }
  }

  private Metric getConnectionCountMetric(Connection connection) throws SQLException {
    try (final PreparedStatement preparedStatement =
        connection.prepareStatement(
            "SELECT count(1) AS count FROM pg_stat_activity WHERE application_name = ?")) {
      preparedStatement.setString(1, applicationNameInCurrentConnection);
      final long count;

      try (final ResultSet resultSet = preparedStatement.executeQuery()) {
        count = resultSet.getLong("count");
      }

      return new Metric(count, Map.of(APP_NAME_LABEL, applicationNameInCurrentConnection));
    }
  }

  @Override
  public Duration reportingInterval() {
    return Duration.ofDays(1);
  }
}
