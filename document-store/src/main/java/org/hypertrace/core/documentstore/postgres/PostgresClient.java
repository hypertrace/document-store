package org.hypertrace.core.documentstore.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PostgresClient {

  private static final Logger log = LoggerFactory.getLogger(PostgresClient.class);
  private static final int VALIDATION_QUERY_TIMEOUT_SECONDS = 5;

  private final int maxConnectionAttempts;
  private final Duration connectionRetryBackoff;
  private final PostgresConnectionPool connectionPool;
  private final PostgresConnectionConfig connectionConfig;

  private int count = 0;
  private Connection connection;

  public PostgresClient(final PostgresConnectionConfig config) {
    this.connectionConfig = config;
    this.maxConnectionAttempts = PostgresDefaults.DEFAULT_MAX_CONNECTION_ATTEMPTS;
    this.connectionRetryBackoff = PostgresDefaults.DEFAULT_CONNECTION_RETRY_BACKOFF;
    this.connectionPool = new PostgresConnectionPool(connectionConfig);
  }

  public synchronized Connection getConnection() {
    try {
      if (connection == null) {
        newConnection();
      } else if (!isConnectionValid(connection)) {
        log.info("The database connection is invalid. Reconnecting...");
        close();
        newConnection();
      }
    } catch (SQLException sqle) {
      throw new RuntimeException(sqle);
    }
    return connection;
  }

  public Connection getPooledConnection() throws SQLException {
    return connectionPool.getConnection();
  }

  public void close() {
    if (connection != null) {
      try {
        log.info("Closing connection #{} to {}", count, connectionConfig.toConnectionString());
        connection.close();
      } catch (SQLException sqle) {
        log.warn("Ignoring error closing connection", sqle);
      } finally {
        connection = null;
      }
    }

    connectionPool.close();
  }

  private boolean isConnectionValid(Connection connection) {
    try {
      if (connection.getMetaData().getJDBCMajorVersion() >= 4) {
        return connection.isValid(VALIDATION_QUERY_TIMEOUT_SECONDS);
      } else {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1");
            ResultSet resultSet = preparedStatement.executeQuery()) {
          return true;
        }
      }
    } catch (SQLException sqle) {
      log.debug("Unable to check if the underlying connection is valid", sqle);
      return false;
    }
  }

  private void newConnection() throws SQLException {
    ++count;
    int attempts = 0;
    while (attempts < maxConnectionAttempts) {
      try {
        ++attempts;
        final String connectionString = connectionConfig.toConnectionString();

        log.info(
            "Attempting(attempt #{}) to open connection #{} to {}",
            attempts,
            count,
            connectionString);
        connection =
            DriverManager.getConnection(connectionString, connectionConfig.buildProperties());
        return;
      } catch (SQLException sqle) {
        attempts++;
        if (attempts < maxConnectionAttempts) {
          log.info(
              "Unable to connect(#{}) to database on attempt {}/{}. Will retry in {} ms.",
              count,
              attempts,
              maxConnectionAttempts,
              connectionRetryBackoff,
              sqle);
          try {
            TimeUnit.MILLISECONDS.sleep(connectionRetryBackoff.toMillis());
          } catch (InterruptedException e) {
            // this is ok because just woke up early
          }
        } else {
          throw sqle;
        }
      }
    }
  }
}
