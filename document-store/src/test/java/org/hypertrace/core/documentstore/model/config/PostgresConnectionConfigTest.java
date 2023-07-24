package org.hypertrace.core.documentstore.model.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Properties;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.junit.jupiter.api.Test;
import org.postgresql.PGProperty;

class PostgresConnectionConfigTest {

  @Test
  void testAllDefaults() {
    final PostgresConnectionConfig config =
        (PostgresConnectionConfig) ConnectionConfig.builder().type(DatabaseType.POSTGRES).build();

    assertEquals("jdbc:postgresql://localhost:5432/postgres", config.toConnectionString());

    final Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), "postgres");
    properties.setProperty(PGProperty.PASSWORD.getName(), "postgres");
    properties.setProperty(PGProperty.APPLICATION_NAME.getName(), "document-store");

    assertEquals(properties, config.buildProperties());
  }

  @Test
  void testAllValuesSet() {
    final String username = "Martian";
    final String password = "secret";
    final String applicationName = "the_solar_system";

    final PostgresConnectionConfig config =
        (PostgresConnectionConfig)
            ConnectionConfig.builder()
                .type(DatabaseType.POSTGRES)
                .host("red.planet")
                .port(7000)
                .database("planets")
                .credentials(
                    ConnectionCredentials.builder().username(username).password(password).build())
                .connectionPoolConfig(
                    ConnectionPoolConfig.builder()
                        .maxConnections(45)
                        .connectionAccessTimeout(Duration.ofMinutes(2))
                        .connectionSurrenderTimeout(Duration.ofHours(1))
                        .build())
                .applicationName(applicationName)
                .build();

    assertEquals("jdbc:postgresql://red.planet:7000/planets", config.toConnectionString());

    final Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), username);
    properties.setProperty(PGProperty.PASSWORD.getName(), password);
    properties.setProperty(PGProperty.APPLICATION_NAME.getName(), applicationName);

    assertEquals(properties, config.buildProperties());
  }

  @Test
  void testEmptyCredentialsAndPoolConfig() {
    final String applicationName = "the_solar_system";

    final PostgresConnectionConfig config =
        (PostgresConnectionConfig)
            ConnectionConfig.builder()
                .type(DatabaseType.POSTGRES)
                .host("red.planet")
                .port(7000)
                .database("planets")
                .credentials(ConnectionCredentials.builder().build())
                .connectionPoolConfig(ConnectionPoolConfig.builder().build())
                .applicationName(applicationName)
                .build();

    assertEquals("jdbc:postgresql://red.planet:7000/planets", config.toConnectionString());

    final Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), "postgres");
    properties.setProperty(PGProperty.PASSWORD.getName(), "postgres");
    properties.setProperty(PGProperty.APPLICATION_NAME.getName(), applicationName);

    assertEquals(properties, config.buildProperties());
  }
}
