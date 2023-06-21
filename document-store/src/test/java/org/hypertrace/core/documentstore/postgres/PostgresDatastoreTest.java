package org.hypertrace.core.documentstore.postgres;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.postgresql.PGProperty;

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
                entry("applicationName", "app1"),
                entry("connectionRetryBackoff", "2 minutes")));
    final PostgresDatastore datastore = new PostgresDatastore();
    datastore.init(config);

    final Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), "u1");
    properties.setProperty(PGProperty.PASSWORD.getName(), "pass");
    properties.setProperty(PGProperty.APPLICATION_NAME.getName(), "app1");

    try (final MockedStatic<DriverManager> driverManager =
        Mockito.mockStatic(DriverManager.class)) {
      driverManager
          .when(
              () ->
                  DriverManager.getConnection(
                      "jdbc:postgresql://localhost:5432/inputDB", properties))
          .thenReturn(mockConnection);
      assertEquals(mockConnection, datastore.getPostgresClient());
    }
  }
}
