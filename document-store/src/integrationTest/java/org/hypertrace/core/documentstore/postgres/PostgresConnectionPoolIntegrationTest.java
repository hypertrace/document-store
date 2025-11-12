package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig;
import org.hypertrace.core.documentstore.model.config.DatabaseType;
import org.hypertrace.core.documentstore.model.config.Endpoint;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class PostgresConnectionPoolIntegrationTest {

  private static GenericContainer<?> postgres;
  private static String host;
  private static int port;

  @BeforeAll
  public static void init() {
    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgres.start();

    host = postgres.getHost();
    port = postgres.getMappedPort(5432);
  }

  @AfterAll
  public static void shutdown() {
    postgres.stop();
  }

  @Test
  public void testGetConnection() throws SQLException {
    final PostgresConnectionConfig config = createTestConfig();
    final PostgresConnectionPool pool = new PostgresConnectionPool(config);

    try (final Connection connection = pool.getConnection()) {
      assertNotNull(connection);
      assertTrue(connection.getAutoCommit(), "Regular connection should have autoCommit=true");
      assertFalse(connection.isClosed());

      // Verify the connection works by executing a simple query
      try (final PreparedStatement stmt = connection.prepareStatement("SELECT 1");
          final ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }

    pool.close();
  }

  @Test
  public void testGetTransactionalConnection() throws SQLException {
    final PostgresConnectionConfig config = createTestConfig();
    final PostgresConnectionPool pool = new PostgresConnectionPool(config);

    try (final Connection connection = pool.getTransactionalConnection()) {
      assertNotNull(connection);
      assertFalse(
          connection.getAutoCommit(), "Transactional connection should have autoCommit=false");
      assertFalse(connection.isClosed());

      // Verify the connection works by executing a simple query
      try (final PreparedStatement stmt = connection.prepareStatement("SELECT 2");
          final ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
      }

      // Verify we can commit manually
      connection.commit();
    }

    pool.close();
  }

  @Test
  public void testBothPoolsIndependent() throws SQLException {
    final PostgresConnectionConfig config = createTestConfig();
    final PostgresConnectionPool pool = new PostgresConnectionPool(config);

    // Get connections from both pools simultaneously
    try (final Connection regularConn = pool.getConnection();
        final Connection transactionalConn = pool.getTransactionalConnection()) {

      assertNotNull(regularConn);
      assertNotNull(transactionalConn);

      // Verify they have different autoCommit settings
      assertTrue(regularConn.getAutoCommit());
      assertFalse(transactionalConn.getAutoCommit());

      // Both should work independently
      try (final PreparedStatement stmt1 = regularConn.prepareStatement("SELECT 'regular'");
          final ResultSet rs1 = stmt1.executeQuery()) {
        assertTrue(rs1.next());
        assertEquals("regular", rs1.getString(1));
      }

      try (final PreparedStatement stmt2 =
              transactionalConn.prepareStatement("SELECT 'transactional'");
          final ResultSet rs2 = stmt2.executeQuery()) {
        assertTrue(rs2.next());
        assertEquals("transactional", rs2.getString(1));
      }

      transactionalConn.commit();
    }

    pool.close();
  }

  @Test
  public void testConnectionPooling() throws SQLException {
    final PostgresConnectionConfig config = createTestConfig();
    final PostgresConnectionPool pool = new PostgresConnectionPool(config);

    // Get and release connections multiple times
    Connection conn1 = pool.getConnection();
    assertNotNull(conn1);
    conn1.close();

    Connection conn2 = pool.getConnection();
    assertNotNull(conn2);
    conn2.close();

    // Verify pooling is working by getting multiple connections from transactional pool
    Connection tConn1 = pool.getTransactionalConnection();
    assertNotNull(tConn1);
    tConn1.close();

    Connection tConn2 = pool.getTransactionalConnection();
    assertNotNull(tConn2);
    tConn2.close();

    pool.close();
  }

  @Test
  public void testTransactionalCommitAndRollback() throws SQLException {
    final PostgresConnectionConfig config = createTestConfig();
    final PostgresConnectionPool pool = new PostgresConnectionPool(config);

    // Create a test table
    try (final Connection setupConn = pool.getTransactionalConnection()) {
      try (final PreparedStatement stmt =
          setupConn.prepareStatement(
              "CREATE TABLE IF NOT EXISTS test_table (id INT PRIMARY KEY, value TEXT)")) {
        stmt.execute();
      }
      setupConn.commit();
    }

    // Test commit
    try (final Connection conn = pool.getTransactionalConnection()) {
      try (final PreparedStatement stmt =
          conn.prepareStatement("INSERT INTO test_table (id, value) VALUES (1, 'test')")) {
        stmt.execute();
      }
      conn.commit();

      // Verify data was committed
      try (final PreparedStatement stmt =
              conn.prepareStatement("SELECT value FROM test_table WHERE id = 1");
          final ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("test", rs.getString(1));
      }
    }

    // Test rollback
    try (final Connection conn = pool.getTransactionalConnection()) {
      try (final PreparedStatement stmt =
          conn.prepareStatement("INSERT INTO test_table (id, value) VALUES (2, 'rollback_me')")) {
        stmt.execute();
      }
      conn.rollback();

      // Verify data was not committed
      try (final PreparedStatement stmt =
              conn.prepareStatement("SELECT value FROM test_table WHERE id = 2");
          final ResultSet rs = stmt.executeQuery()) {
        assertFalse(rs.next(), "Data should have been rolled back");
      }
    }

    // Cleanup
    try (final Connection cleanupConn = pool.getTransactionalConnection()) {
      try (final PreparedStatement stmt =
          cleanupConn.prepareStatement("DROP TABLE IF EXISTS test_table")) {
        stmt.execute();
      }
      cleanupConn.commit();
    }

    pool.close();
  }

  @Test
  public void testClose() throws SQLException {
    final PostgresConnectionConfig config = createTestConfig();
    final PostgresConnectionPool pool = new PostgresConnectionPool(config);

    // Get connections to ensure pools are active
    final Connection regularConnection = pool.getConnection();
    final Connection transactionalConnection = pool.getTransactionalConnection();

    assertNotNull(regularConnection);
    assertNotNull(transactionalConnection);

    // Close the connections back to the pool
    regularConnection.close();
    transactionalConnection.close();

    // Close the pool - should not throw
    pool.close();

    // After closing the pool, trying to get connections should fail with IllegalStateException
    assertThrows(IllegalStateException.class, pool::getConnection);
    assertThrows(IllegalStateException.class, pool::getTransactionalConnection);
  }

  @Test
  public void testCloseIdempotent() throws SQLException {
    final PostgresConnectionConfig config = createTestConfig();
    final PostgresConnectionPool pool = new PostgresConnectionPool(config);

    // First close
    pool.close();

    // Second close should not throw
    pool.close();
  }

  private static PostgresConnectionConfig createTestConfig() {
    return (PostgresConnectionConfig)
        ConnectionConfig.builder()
            .type(DatabaseType.POSTGRES)
            .addEndpoint(Endpoint.builder().host(host).port(port).build())
            .database("postgres")
            .credentials(
                ConnectionCredentials.builder().username("postgres").password("postgres").build())
            .connectionPoolConfig(
                ConnectionPoolConfig.builder()
                    .maxConnections(5)
                    .connectionAccessTimeout(Duration.ofSeconds(10))
                    .connectionSurrenderTimeout(Duration.ofSeconds(30))
                    .build())
            .build();
  }
}
