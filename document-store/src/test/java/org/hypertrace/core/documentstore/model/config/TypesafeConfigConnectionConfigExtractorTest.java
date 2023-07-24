package org.hypertrace.core.documentstore.model.config;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TypesafeConfigConnectionConfigExtractorTest {
  private static final String TYPE_KEY = "database_type";
  private static final String HOST_KEY = "hostname";
  private static final String PORT_KEY = "port_number";
  private static final String DATABASE_KEY = "database_name";
  private static final String USER_KEY = "username";
  private static final String PASSWORD_KEY = "pword";
  private static final String AUTH_DB_KEY = "authenticationDatabaseName";
  private static final String APP_NAME_KEY = "applicationNameInFull";
  private static final String MAX_CONNECTIONS_KEY = "maxConnectionsKey";
  private static final String CONNECTION_ACCESS_TIMEOUT_KEY = "connectionAccessTimeout";
  private static final String CONNECTION_SURRENDER_TIMEOUT_KEY = "connectionSurrenderTimeout";

  private static final String host = "red.planet";
  private static final int port = 4;
  private static final String database = "planets";
  private static final String user = "Martian";
  private static final String password = ".--..-";
  private static final String authDb = "sun_planets";
  private static final String appName = "the_solar_system";
  private static final int maxConnections = 7;
  private static final Duration accessTimeout = Duration.ofSeconds(67);
  private static final Duration surrenderTimeout = Duration.ofSeconds(56);

  @Test
  void testMandatoryFields() {
    assertThrows(
        NullPointerException.class,
        () -> TypesafeConfigConnectionConfigExtractor.from(null, "type"));
    assertThrows(
        NullPointerException.class,
        () -> TypesafeConfigConnectionConfigExtractor.from(ConfigFactory.empty(), (String) null));
    assertThrows(
        NullPointerException.class,
        () ->
            TypesafeConfigConnectionConfigExtractor.from(
                ConfigFactory.empty(), (DatabaseType) null));
  }

  @Test
  void testInvalidTypeKey() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TypesafeConfigConnectionConfigExtractor.from(ConfigFactory.empty(), TYPE_KEY));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TypesafeConfigConnectionConfigExtractor.from(
                ConfigFactory.parseMap(Map.of(TYPE_KEY, "invalid")), TYPE_KEY));
  }

  @ParameterizedTest
  @EnumSource(value = DatabaseType.class)
  void testAllEmpty(final DatabaseType type) {
    final ConnectionConfig config =
        TypesafeConfigConnectionConfigExtractor.from(ConfigFactory.empty(), type).extract();
    final ConnectionConfig expected = ConnectionConfig.builder().type(type).build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildMongo() {
    final ConnectionConfig config =
        TypesafeConfigConnectionConfigExtractor.from(buildConfigMap(), DatabaseType.MONGO)
            .hostKey(HOST_KEY)
            .portKey(PORT_KEY)
            .databaseKey(DATABASE_KEY)
            .usernameKey(USER_KEY)
            .passwordKey(PASSWORD_KEY)
            .authDatabaseKey(AUTH_DB_KEY)
            .applicationNameKey(APP_NAME_KEY)
            .poolMaxConnectionsKey(MAX_CONNECTIONS_KEY)
            .poolConnectionAccessTimeoutKey(CONNECTION_ACCESS_TIMEOUT_KEY)
            .poolConnectionSurrenderTimeoutKey(CONNECTION_SURRENDER_TIMEOUT_KEY)
            .extract();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.MONGO)
            .host(host)
            .port(port)
            .database(database)
            .credentials(
                ConnectionCredentials.builder()
                    .username(user)
                    .password(password)
                    .authDatabase(authDb)
                    .build())
            .applicationName(appName)
            .build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildPostgres() {
    final ConnectionConfig config =
        TypesafeConfigConnectionConfigExtractor.from(buildConfigMap(), DatabaseType.POSTGRES)
            .hostKey(HOST_KEY)
            .portKey(PORT_KEY)
            .databaseKey(DATABASE_KEY)
            .usernameKey(USER_KEY)
            .passwordKey(PASSWORD_KEY)
            .authDatabaseKey(AUTH_DB_KEY)
            .applicationNameKey(APP_NAME_KEY)
            .poolMaxConnectionsKey(MAX_CONNECTIONS_KEY)
            .poolConnectionAccessTimeoutKey(CONNECTION_ACCESS_TIMEOUT_KEY)
            .poolConnectionSurrenderTimeoutKey(CONNECTION_SURRENDER_TIMEOUT_KEY)
            .extract();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.POSTGRES)
            .host(host)
            .port(port)
            .database(database)
            .credentials(
                ConnectionCredentials.builder()
                    .username(user)
                    .password(password)
                    .authDatabase(authDb)
                    .build())
            .applicationName(appName)
            .connectionPoolConfig(
                ConnectionPoolConfig.builder()
                    .maxConnections(maxConnections)
                    .connectionAccessTimeout(accessTimeout)
                    .connectionSurrenderTimeout(surrenderTimeout)
                    .build())
            .build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildMongoUsingTypeKey() {
    final ConnectionConfig config =
        TypesafeConfigConnectionConfigExtractor.from(buildConfigMap("mongo"), TYPE_KEY)
            .hostKey(HOST_KEY)
            .portKey(PORT_KEY)
            .databaseKey(DATABASE_KEY)
            .usernameKey(USER_KEY)
            .passwordKey(PASSWORD_KEY)
            .authDatabaseKey(AUTH_DB_KEY)
            .applicationNameKey(APP_NAME_KEY)
            .poolMaxConnectionsKey(MAX_CONNECTIONS_KEY)
            .poolConnectionAccessTimeoutKey(CONNECTION_ACCESS_TIMEOUT_KEY)
            .poolConnectionSurrenderTimeoutKey(CONNECTION_SURRENDER_TIMEOUT_KEY)
            .extract();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.MONGO)
            .host(host)
            .port(port)
            .database(database)
            .credentials(
                ConnectionCredentials.builder()
                    .username(user)
                    .password(password)
                    .authDatabase(authDb)
                    .build())
            .applicationName(appName)
            .build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildPostgresUsingTypeKey() {
    final ConnectionConfig config =
        TypesafeConfigConnectionConfigExtractor.from(buildConfigMap("postgres"), TYPE_KEY)
            .hostKey(HOST_KEY)
            .portKey(PORT_KEY)
            .databaseKey(DATABASE_KEY)
            .usernameKey(USER_KEY)
            .passwordKey(PASSWORD_KEY)
            .authDatabaseKey(AUTH_DB_KEY)
            .applicationNameKey(APP_NAME_KEY)
            .poolMaxConnectionsKey(MAX_CONNECTIONS_KEY)
            .poolConnectionAccessTimeoutKey(CONNECTION_ACCESS_TIMEOUT_KEY)
            .poolConnectionSurrenderTimeoutKey(CONNECTION_SURRENDER_TIMEOUT_KEY)
            .extract();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.POSTGRES)
            .host(host)
            .port(port)
            .database(database)
            .credentials(
                ConnectionCredentials.builder()
                    .username(user)
                    .password(password)
                    .authDatabase(authDb)
                    .build())
            .applicationName(appName)
            .connectionPoolConfig(
                ConnectionPoolConfig.builder()
                    .maxConnections(maxConnections)
                    .connectionAccessTimeout(accessTimeout)
                    .connectionSurrenderTimeout(surrenderTimeout)
                    .build())
            .build();

    assertEquals(expected, config);
  }

  private Config buildConfigMap() {
    return ConfigFactory.parseMap(
        Map.ofEntries(
            entry(HOST_KEY, host),
            entry(PORT_KEY, port),
            entry(DATABASE_KEY, database),
            entry(USER_KEY, user),
            entry(PASSWORD_KEY, password),
            entry(AUTH_DB_KEY, authDb),
            entry(APP_NAME_KEY, appName),
            entry(MAX_CONNECTIONS_KEY, maxConnections),
            entry(CONNECTION_ACCESS_TIMEOUT_KEY, accessTimeout),
            entry(CONNECTION_SURRENDER_TIMEOUT_KEY, surrenderTimeout)));
  }

  private Config buildConfigMap(final String databaseType) {
    return ConfigFactory.parseMap(
        Map.ofEntries(
            entry(TYPE_KEY, databaseType),
            entry(HOST_KEY, host),
            entry(PORT_KEY, port),
            entry(DATABASE_KEY, database),
            entry(USER_KEY, user),
            entry(PASSWORD_KEY, password),
            entry(AUTH_DB_KEY, authDb),
            entry(APP_NAME_KEY, appName),
            entry(MAX_CONNECTIONS_KEY, maxConnections),
            entry(CONNECTION_ACCESS_TIMEOUT_KEY, accessTimeout),
            entry(CONNECTION_SURRENDER_TIMEOUT_KEY, surrenderTimeout)));
  }
}
