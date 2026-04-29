package org.hypertrace.core.documentstore.postgres;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;

@Slf4j
class PostgresConnectionPool {

  private static final String VALIDATION_QUERY = "SELECT 1";
  private static final Duration VALIDATION_QUERY_TIMEOUT = Duration.ofSeconds(5);

  // Single pool with autoCommit=true by default. Transactional connections flip autoCommit
  // to false on borrow; DBCP2 resets it back when the connection is returned to the pool.
  private final PoolingDataSource<PoolableConnection> dataSource;

  PostgresConnectionPool(final PostgresConnectionConfig config) {
    this.dataSource = createPooledDataSource(config);
  }

  /**
   * Get a connection with autoCommit=true (pool default). Use for simple queries that don't need
   * manual transaction management.
   */
  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  /**
   * Get a connection with autoCommit=false for manual transaction management (commit/rollback).
   * When the connection is closed/returned to the pool, DBCP2 automatically resets autoCommit back
   * to the pool default (true).
   */
  public Connection getTransactionalConnection() throws SQLException {
    Connection conn = dataSource.getConnection();
    conn.setAutoCommit(false);
    return conn;
  }

  public void close() {
    try {
      dataSource.close();
    } catch (final SQLException e) {
      log.warn("Unable to close Postgres connection pool", e);
    }
  }

  private PoolingDataSource<PoolableConnection> createPooledDataSource(
      final PostgresConnectionConfig config) {
    final ConnectionFactory connectionFactory =
        new DriverManagerConnectionFactory(config.toConnectionString(), config.buildProperties());
    final PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);
    final GenericObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory);

    final ConnectionPoolConfig poolConfig = config.connectionPoolConfig();
    setPoolProperties(connectionPool, poolConfig);
    setFactoryProperties(poolableConnectionFactory, connectionPool);

    return new PoolingDataSource<>(connectionPool);
  }

  private void setPoolProperties(
      final GenericObjectPool<PoolableConnection> connectionPool,
      final ConnectionPoolConfig poolConfig) {
    final AbandonedConfig abandonedConfig = getAbandonedConfig(poolConfig);
    final int maxConnections = poolConfig.maxConnections();
    connectionPool.setMaxTotal(maxConnections);
    connectionPool.setMaxIdle(getIdleCount(poolConfig.maxIdlePercent(), maxConnections));
    connectionPool.setMinIdle(getIdleCount(poolConfig.minIdlePercent(), maxConnections));
    connectionPool.setBlockWhenExhausted(true);
    connectionPool.setMaxWaitMillis(poolConfig.connectionAccessTimeout().toMillis());
    connectionPool.setAbandonedConfig(abandonedConfig);
    log.debug(
        "Postgres connection pool properties - maxTotal: {}, maxIdle: {}, minIdle: {}, maxWaitMillis: {}, connectionSurrenderTimeout: {}",
        connectionPool.getMaxTotal(),
        connectionPool.getMaxIdle(),
        connectionPool.getMinIdle(),
        connectionPool.getMaxWaitMillis(),
        poolConfig.connectionSurrenderTimeout());
  }

  private void setFactoryProperties(
      PoolableConnectionFactory poolableConnectionFactory,
      GenericObjectPool<PoolableConnection> connectionPool) {
    poolableConnectionFactory.setPool(connectionPool);
    poolableConnectionFactory.setValidationQuery(VALIDATION_QUERY);
    poolableConnectionFactory.setValidationQueryTimeout((int) VALIDATION_QUERY_TIMEOUT.toSeconds());
    poolableConnectionFactory.setDefaultReadOnly(false);
    poolableConnectionFactory.setDefaultAutoCommit(true);
    poolableConnectionFactory.setDefaultTransactionIsolation(TRANSACTION_READ_COMMITTED);
    poolableConnectionFactory.setPoolStatements(false);
  }

  private AbandonedConfig getAbandonedConfig(final ConnectionPoolConfig poolConfig) {
    final AbandonedConfig abandonedConfig = new AbandonedConfig();
    abandonedConfig.setLogAbandoned(true);
    abandonedConfig.setRemoveAbandonedOnBorrow(true);
    abandonedConfig.setRequireFullStackTrace(true);
    abandonedConfig.setRemoveAbandonedTimeout(poolConfig.connectionSurrenderTimeout());
    return abandonedConfig;
  }

  private int getIdleCount(final int percent, final int maxConnections) {
    if (percent < 0) {
      return maxConnections;
    }
    final int value = (maxConnections * percent) / 100;
    return Math.max(value, 1);
  }
}
