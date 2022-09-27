package org.hypertrace.core.documentstore.postgres;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostgresDatastoreTest {
  @Mock private Connection mockConnection;

  @Test
  void initWithHostAndPort() {
    final Config config =
        ConfigFactory.parseMap(
            Map.ofEntries(
                entry("database", "inputDB"),
                entry("host", "localhost"),
                entry("port", "5432"),
                entry("user", "u1"),
                entry("password", "pass"),
                entry("maxConnectionAttempts", "7"),
                entry("connectionRetryBackoff", "2 minutes")));
    final PostgresDatastore datastore = new PostgresDatastore();
    datastore.init(config);

    try (final MockedStatic<DriverManager> driverManager =
        Mockito.mockStatic(DriverManager.class)) {
      driverManager
          .when(
              () ->
                  DriverManager.getConnection(
                      "jdbc:postgresql://localhost:5432/inputDB", "u1", "pass"))
          .thenReturn(mockConnection);
      assertEquals(mockConnection, datastore.getPostgresClient());
    }
  }

  @Test
  void initWithConnectionString() {
    final Config config =
        ConfigFactory.parseMap(
            Map.ofEntries(
                entry("database", "inputDB"),
                entry("host", "localhost"),
                entry("port", "5432"),
                entry("url", "connectionString/"),
                entry("user", "u1"),
                entry("password", "pass"),
                entry("maxConnectionAttempts", "7"),
                entry("connectionRetryBackoff", "2 minutes")));
    final PostgresDatastore datastore = new PostgresDatastore();
    datastore.init(config);

    try (final MockedStatic<DriverManager> driverManager =
        Mockito.mockStatic(DriverManager.class)) {
      driverManager
          .when(() -> DriverManager.getConnection("connectionString/inputDB", "u1", "pass"))
          .thenReturn(mockConnection);
      assertEquals(mockConnection, datastore.getPostgresClient());
    }
  }

  @Test
  void initWithDefaults() {
    final Config config =
        ConfigFactory.parseMap(Map.ofEntries(entry("host", "localhost"), entry("port", "5432")));
    final PostgresDatastore datastore = new PostgresDatastore();
    datastore.init(config);

    try (final MockedStatic<DriverManager> driverManager =
        Mockito.mockStatic(DriverManager.class)) {
      driverManager
          .when(
              () ->
                  DriverManager.getConnection(
                      "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres"))
          .thenReturn(mockConnection);
      assertEquals(mockConnection, datastore.getPostgresClient());
    }
  }
}
