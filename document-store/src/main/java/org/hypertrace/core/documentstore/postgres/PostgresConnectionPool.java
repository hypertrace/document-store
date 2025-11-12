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

  // data source for pools with auto-commits enabled
  private final PoolingDataSource<PoolableConnection> regularDataSource;
  // data source for pools with auto-commits disabled. This is used for transactional operations
  // that manage their own commit logic
  private final PoolingDataSource<PoolableConnection> transactionalDataSource;

  PostgresConnectionPool(final PostgresConnectionConfig config) {
    this.regularDataSource = createPooledDataSource(config, true);
    this.transactionalDataSource = createPooledDataSource(config, false);
  }

  /**
   * Get a connection from the regular pool with autoCommit=true. Use for read-only queries that
   * don't need manual transaction management.
   */
  public Connection getConnection() throws SQLException {
    return regularDataSource.getConnection();
  }

  /**
   * Get a connection from the transactional pool with autoCommit=false. Use for operations that
   * require manual transaction management (commit/rollback).
   */
  public Connection getTransactionalConnection() throws SQLException {
    return transactionalDataSource.getConnection();
  }

  public void close() {
    try {
      regularDataSource.close();
    } catch (final SQLException e) {
      log.warn("Unable to close regular Postgres connection pool", e);
    }
    try {
      transactionalDataSource.close();
    } catch (final SQLException e) {
      log.warn("Unable to close transactional Postgres connection pool", e);
    }
  }

  private PoolingDataSource<PoolableConnection> createPooledDataSource(
      final PostgresConnectionConfig config, final boolean autoCommit) {
    final ConnectionFactory connectionFactory =
        new DriverManagerConnectionFactory(config.toConnectionString(), config.buildProperties());
    final PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);
    final GenericObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory);

    final ConnectionPoolConfig poolConfig = config.connectionPoolConfig();
    setPoolProperties(connectionPool, poolConfig);
    setFactoryProperties(poolableConnectionFactory, connectionPool, autoCommit);

    return new PoolingDataSource<>(connectionPool);
  }

  private void setPoolProperties(
      final GenericObjectPool<PoolableConnection> connectionPool,
      final ConnectionPoolConfig poolConfig) {
    final AbandonedConfig abandonedConfig = getAbandonedConfig(poolConfig);
    final int maxConnections = poolConfig.maxConnections();
    connectionPool.setMaxTotal(maxConnections);
    // max idle connections are 20% of max connections
    connectionPool.setMaxIdle(getPercentOf(20, maxConnections));
    // min idle connections are 10% of max connections
    connectionPool.setMinIdle(getPercentOf(10, maxConnections));
    connectionPool.setBlockWhenExhausted(true);
    connectionPool.setMaxWaitMillis(poolConfig.connectionAccessTimeout().toMillis());
    connectionPool.setAbandonedConfig(abandonedConfig);
  }

  private void setFactoryProperties(
      PoolableConnectionFactory poolableConnectionFactory,
      GenericObjectPool<PoolableConnection> connectionPool,
      boolean autoCommit) {
    poolableConnectionFactory.setPool(connectionPool);
    poolableConnectionFactory.setValidationQuery(VALIDATION_QUERY);
    poolableConnectionFactory.setValidationQueryTimeout((int) VALIDATION_QUERY_TIMEOUT.toSeconds());
    poolableConnectionFactory.setDefaultReadOnly(false);
    poolableConnectionFactory.setDefaultAutoCommit(autoCommit);
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

  private int getPercentOf(final int percent, final int maxConnections) {
    final int value = (maxConnections * percent) / 100;
    return Math.max(value, 1);
  }
}
