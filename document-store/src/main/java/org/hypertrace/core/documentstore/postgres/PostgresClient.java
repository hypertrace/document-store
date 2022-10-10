package org.hypertrace.core.documentstore.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PostgresClient {

  private static final Logger log = LoggerFactory.getLogger(PostgresClient.class);
  private static final int VALIDATION_QUERY_TIMEOUT_SECONDS = 5;

  private final String url;
  private final String user;
  private final String password;
  private final int maxConnectionAttempts;
  private final Duration connectionRetryBackoff;
  private final PostgresConnectionPool connectionPool;

  private int count = 0;
  private Connection connection;

  public PostgresClient(final PostgresConfig config) {
    this.url = config.getConnectionString();
    this.user = config.getUser();
    this.password = config.getPassword();
    this.maxConnectionAttempts = config.getMaxConnectionAttempts();
    this.connectionRetryBackoff = config.getConnectionRetryBackoff();
    this.connectionPool = new PostgresConnectionPool(config);
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
        log.info("Attempting(attempt #{}) to open connection #{} to {}", attempts, count, url);
        connection = DriverManager.getConnection(url, user, password);
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

  private void close() {
    if (connection != null) {
      try {
        log.info("Closing connection #{} to {}", count, url);
        connection.close();
      } catch (SQLException sqle) {
        log.warn("Ignoring error closing connection", sqle);
      } finally {
        connection = null;
      }
    }
  }
}
