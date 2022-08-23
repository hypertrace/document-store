package org.hypertrace.core.documentstore.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;

class PostgresClient {

  private final DataSource dataSource;

  PostgresClient(
      String url,
      String user,
      String password,
      int maxConnections,
      Duration maxWaitTime,
      Duration removeAbandonedTimeout) {
    this.dataSource =
        createPooledDataSource(
            url, user, password, maxConnections, maxWaitTime, removeAbandonedTimeout);
  }

  private DataSource createPooledDataSource(
      String url,
      String user,
      String password,
      int maxConnections,
      Duration maxWaitTime,
      Duration removeAbandonedTimeout) {
    ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, user, password);
    PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);
    GenericObjectPool<PoolableConnection> connectionPool =
        new GenericObjectPool<>(poolableConnectionFactory);
    connectionPool.setMaxTotal(maxConnections);
    // max idle connections are 20% of max connections
    connectionPool.setMaxIdle(getPercentOf(maxConnections, 20));
    // min idle connections are 10% of max connections
    connectionPool.setMinIdle(getPercentOf(maxConnections, 10));
    connectionPool.setBlockWhenExhausted(true);
    connectionPool.setMaxWaitMillis(maxWaitTime.toMillis());

    // set the abandoned config for connection pool
    AbandonedConfig abandonedConfig = new AbandonedConfig();
    abandonedConfig.setLogAbandoned(true);
    abandonedConfig.setRemoveAbandonedOnBorrow(true);
    abandonedConfig.setRequireFullStackTrace(true);
    abandonedConfig.setRemoveAbandonedTimeout(removeAbandonedTimeout);
    connectionPool.setAbandonedConfig(abandonedConfig);

    poolableConnectionFactory.setPool(connectionPool);
    poolableConnectionFactory.setValidationQuery("SELECT 1");
    poolableConnectionFactory.setValidationQueryTimeout(5);
    poolableConnectionFactory.setDefaultReadOnly(false);
    poolableConnectionFactory.setDefaultAutoCommit(true);
    poolableConnectionFactory.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    poolableConnectionFactory.setPoolStatements(false);
    return new PoolingDataSource<>(connectionPool);
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  private int getPercentOf(int maxConnections, int percent) {
    int value = (maxConnections * percent) / 100;
    // minimum value should be 1
    return Math.max(value, 1);
  }
}
