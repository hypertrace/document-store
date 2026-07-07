package org.hypertrace.core.documentstore.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;

@Slf4j
class PostgresConnectionPool {

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
    setFactoryProperties(poolableConnectionFactory, connectionPool, poolConfig);

    return new PoolingDataSource<>(connectionPool);
  }

  private void setPoolProperties(
      final GenericObjectPool<PoolableConnection> connectionPool,
      final ConnectionPoolConfig poolConfig) {
    final int maxConnections = poolConfig.maxConnections();
    connectionPool.setMaxTotal(maxConnections);
    connectionPool.setMaxIdle(getIdleCount(poolConfig.maxIdlePercent(), maxConnections));
    connectionPool.setMinIdle(getIdleCount(poolConfig.minIdlePercent(), maxConnections));
    connectionPool.setBlockWhenExhausted(true);
    connectionPool.setMaxWaitMillis(poolConfig.connectionAccessTimeout().toMillis());
    if (poolConfig.testOnCreate() != null) {
      connectionPool.setTestOnCreate(poolConfig.testOnCreate());
    }
    connectionPool.setTestOnBorrow(poolConfig.testOnBorrow());
    if (poolConfig.testOnReturn() != null) {
      connectionPool.setTestOnReturn(poolConfig.testOnReturn());
    }
    if (poolConfig.testWhileIdle() != null) {
      connectionPool.setTestWhileIdle(poolConfig.testWhileIdle());
    }
    if (poolConfig.timeBetweenEvictionRuns() != null) {
      connectionPool.setTimeBetweenEvictionRuns(poolConfig.timeBetweenEvictionRuns());
    }
    if (poolConfig.numTestsPerEvictionRun() != null) {
      connectionPool.setNumTestsPerEvictionRun(poolConfig.numTestsPerEvictionRun());
    }
    if (poolConfig.minEvictableIdleTime() != null) {
      connectionPool.setMinEvictableIdleTime(poolConfig.minEvictableIdleTime());
    }
    if (poolConfig.softMinEvictableIdleTime() != null) {
      connectionPool.setSoftMinEvictableIdleTime(poolConfig.softMinEvictableIdleTime());
    }
    if (poolConfig.lifo() != null) {
      connectionPool.setLifo(poolConfig.lifo());
    }
    log.debug(
        "Postgres connection pool properties - maxTotal: {}, maxIdle: {}, minIdle: {}, maxWaitMillis: {}, testOnBorrow: {}, testWhileIdle: {}, timeBetweenEvictionRunsMillis: {}, minEvictableIdleTimeMillis: {}, maxConnLifetimeMillis: {}, lifo: {}",
        connectionPool.getMaxTotal(),
        connectionPool.getMaxIdle(),
        connectionPool.getMinIdle(),
        connectionPool.getMaxWaitMillis(),
        poolConfig.testOnBorrow(),
        poolConfig.testWhileIdle(),
        toMillisOrNull(poolConfig.timeBetweenEvictionRuns()),
        toMillisOrNull(poolConfig.minEvictableIdleTime()),
        toMillisOrNull(poolConfig.maxConnLifetime()),
        poolConfig.lifo());
  }

  private static Long toMillisOrNull(final Duration duration) {
    return duration == null ? null : duration.toMillis();
  }

  private void setFactoryProperties(
      PoolableConnectionFactory poolableConnectionFactory,
      GenericObjectPool<PoolableConnection> connectionPool,
      ConnectionPoolConfig poolConfig) {
    poolableConnectionFactory.setPool(connectionPool);
    if (poolConfig.validationQuery() != null) {
      poolableConnectionFactory.setValidationQuery(poolConfig.validationQuery());
    }
    if (poolConfig.validationQueryTimeout() != null) {
      poolableConnectionFactory.setValidationQueryTimeout(
          (int) poolConfig.validationQueryTimeout().toSeconds());
    }
    poolableConnectionFactory.setDefaultReadOnly(false);
    poolableConnectionFactory.setDefaultAutoCommit(true);
    if (poolConfig.maxConnLifetime() != null) {
      poolableConnectionFactory.setMaxConnLifetimeMillis(poolConfig.maxConnLifetime().toMillis());
    }
    poolableConnectionFactory.setConnectionInitSql(poolConfig.connectionInitSqls());
    // Note: We intentionally do NOT call setDefaultTransactionIsolation() here.
    // PostgreSQL defaults to READ_COMMITTED, which is what we want. Setting it explicitly
    // causes DBCP2 to execute "SHOW TRANSACTION ISOLATION LEVEL" on every connection borrow
    // (via PgConnection.getTransactionIsolation()), adding unnecessary overhead.
    poolableConnectionFactory.setPoolStatements(false);
  }

  private int getIdleCount(final int percent, final int maxConnections) {
    if (percent < 0) {
      return maxConnections;
    }
    final int value = (maxConnections * percent) / 100;
    return Math.max(value, 1);
  }
}
