package org.hypertrace.core.documentstore.model.config;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.model.config.AggregatePipelineMode.SORT_OPTIMIZED_IF_POSSIBLE;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.NEAR_REALTIME_FRESHNESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.model.options.DataFreshness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TypesafeConfigDatastoreConfigExtractorTest {
  private static final String TYPE_KEY = "database_type";
  private static final String HOST_KEY = "hostname";
  private static final String PORT_KEY = "port_number";
  private static final String ENDPOINTS_KEY = "endpoints";
  private static final String DATABASE_KEY = "database_name";
  private static final String USER_KEY = "username";
  private static final String PASSWORD_KEY = "pword";
  private static final String AUTH_DB_KEY = "authenticationDatabaseName";
  private static final String APP_NAME_KEY = "applicationNameInFull";
  private static final String REPLICA_SET_KEY = "replicaSetName";
  private static final String MAX_CONNECTIONS_KEY = "maxConnectionsKey";
  private static final String CONNECTION_ACCESS_TIMEOUT_KEY = "connectionAccessTimeout";
  private static final String CONNECTION_SURRENDER_TIMEOUT_KEY = "connectionSurrenderTimeout";
  private static final String AGGREGATION_PIPELINE_MODE_KEY = "aggregationPipelineMode";
  private static final String DATA_FRESHNESS_KEY = "dataFreshness";
  private static final String QUERY_TIMEOUT_KEY = "queryTimeout";
  private static final String CONNECTION_TIMEOUT_KEY = "connectionTimeout";

  private static final String host = "red.planet";
  private static final String host1 = "RED_PLANET";
  private static final String host2 = "THE_FUTURE_WORLD";
  private static final int port = 4;
  private static final String database = "planets";
  private static final String user = "Martian";
  private static final String password = ".--..-";
  private static final String authDb = "sun_planets";
  private static final String appName = "the_solar_system";
  private static final String replicaSet = "Milky_way";
  private static final int maxConnections = 7;
  private static final Duration accessTimeout = Duration.ofSeconds(67);
  private static final Duration surrenderTimeout = Duration.ofSeconds(56);
  private static final AggregatePipelineMode aggregatePipelineMode = SORT_OPTIMIZED_IF_POSSIBLE;
  private static final DataFreshness dataFreshness = NEAR_REALTIME_FRESHNESS;
  private static final Duration queryTimeout = Duration.ofSeconds(45);
  private static final Duration connectionTimeout = Duration.ofSeconds(30);

  @SuppressWarnings("ConstantConditions")
  @Test
  void testMandatoryFields() {
    assertThrows(
        NullPointerException.class,
        () -> TypesafeConfigDatastoreConfigExtractor.from(null, "type"));
    assertThrows(
        NullPointerException.class,
        () -> TypesafeConfigDatastoreConfigExtractor.from(ConfigFactory.empty(), (String) null));
    assertThrows(
        NullPointerException.class,
        () ->
            TypesafeConfigDatastoreConfigExtractor.from(
                ConfigFactory.empty(), (DatabaseType) null));
  }

  @Test
  void testInvalidTypeKey() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TypesafeConfigDatastoreConfigExtractor.from(ConfigFactory.empty(), TYPE_KEY));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TypesafeConfigDatastoreConfigExtractor.from(
                ConfigFactory.parseMap(Map.of(TYPE_KEY, "invalid")), TYPE_KEY));
  }

  @ParameterizedTest
  @EnumSource(value = DatabaseType.class)
  void testAllEmpty(final DatabaseType type) {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(ConfigFactory.empty(), type)
            .extract()
            .connectionConfig();
    final ConnectionConfig expected = ConnectionConfig.builder().type(type).build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildMongo() {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(buildConfigMap(), DatabaseType.MONGO)
            .hostKey(HOST_KEY)
            .portKey(PORT_KEY)
            .databaseKey(DATABASE_KEY)
            .usernameKey(USER_KEY)
            .passwordKey(PASSWORD_KEY)
            .authDatabaseKey(AUTH_DB_KEY)
            .applicationNameKey(APP_NAME_KEY)
            .replicaSetKey(REPLICA_SET_KEY)
            .poolMaxConnectionsKey(MAX_CONNECTIONS_KEY)
            .poolConnectionAccessTimeoutKey(CONNECTION_ACCESS_TIMEOUT_KEY)
            .poolConnectionSurrenderTimeoutKey(CONNECTION_SURRENDER_TIMEOUT_KEY)
            .aggregationPipelineMode(AGGREGATION_PIPELINE_MODE_KEY)
            .dataFreshnessKey(DATA_FRESHNESS_KEY)
            .queryTimeoutKey(QUERY_TIMEOUT_KEY)
            .connectionTimeoutKey(CONNECTION_TIMEOUT_KEY)
            .extract()
            .connectionConfig();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.MONGO)
            .addEndpoint(Endpoint.builder().host(host).port(port).build())
            .database(database)
            .replicaSet(replicaSet)
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
            .aggregationPipelineMode(aggregatePipelineMode)
            .dataFreshness(dataFreshness)
            .aggregationPipelineMode(aggregatePipelineMode)
            .queryTimeout(queryTimeout)
            .connectionTimeout(connectionTimeout)
            .build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildPostgres() {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(buildConfigMap(), DatabaseType.POSTGRES)
            .hostKey(HOST_KEY)
            .portKey(PORT_KEY)
            .databaseKey(DATABASE_KEY)
            .usernameKey(USER_KEY)
            .passwordKey(PASSWORD_KEY)
            .authDatabaseKey(AUTH_DB_KEY)
            .applicationNameKey(APP_NAME_KEY)
            .replicaSetKey(REPLICA_SET_KEY)
            .poolMaxConnectionsKey(MAX_CONNECTIONS_KEY)
            .poolConnectionAccessTimeoutKey(CONNECTION_ACCESS_TIMEOUT_KEY)
            .poolConnectionSurrenderTimeoutKey(CONNECTION_SURRENDER_TIMEOUT_KEY)
            .queryTimeoutKey(QUERY_TIMEOUT_KEY)
            .extract()
            .connectionConfig();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.POSTGRES)
            .addEndpoint(Endpoint.builder().host(host).port(port).build())
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
            .aggregationPipelineMode(SORT_OPTIMIZED_IF_POSSIBLE)
            .queryTimeout(queryTimeout)
            .build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildMongoUsingTypeKey() {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(
                buildMongoConfigMapUsingEndpointsKey(), TYPE_KEY)
            .keysForEndpoints(ENDPOINTS_KEY, HOST_KEY, PORT_KEY)
            .databaseKey(DATABASE_KEY)
            .usernameKey(USER_KEY)
            .passwordKey(PASSWORD_KEY)
            .authDatabaseKey(AUTH_DB_KEY)
            .applicationNameKey(APP_NAME_KEY)
            .replicaSetKey(REPLICA_SET_KEY)
            .poolMaxConnectionsKey(MAX_CONNECTIONS_KEY)
            .poolConnectionAccessTimeoutKey(CONNECTION_ACCESS_TIMEOUT_KEY)
            .poolConnectionSurrenderTimeoutKey(CONNECTION_SURRENDER_TIMEOUT_KEY)
            .extract()
            .connectionConfig();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.MONGO)
            .addEndpoint(Endpoint.builder().host(host1).port(27017).build())
            .addEndpoint(Endpoint.builder().host(host2).port(port).build())
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
            .replicaSet(replicaSet)
            .build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildPostgresUsingTypeKey() {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(
                buildPostgresConfigMapUsingEndpointsKey(), TYPE_KEY)
            .keysForEndpoints(ENDPOINTS_KEY, HOST_KEY, PORT_KEY)
            .databaseKey(DATABASE_KEY)
            .usernameKey(USER_KEY)
            .passwordKey(PASSWORD_KEY)
            .authDatabaseKey(AUTH_DB_KEY)
            .applicationNameKey(APP_NAME_KEY)
            .replicaSetKey(REPLICA_SET_KEY)
            .poolMaxConnectionsKey(MAX_CONNECTIONS_KEY)
            .poolConnectionAccessTimeoutKey(CONNECTION_ACCESS_TIMEOUT_KEY)
            .poolConnectionSurrenderTimeoutKey(CONNECTION_SURRENDER_TIMEOUT_KEY)
            .extract()
            .connectionConfig();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.POSTGRES)
            .addEndpoint(Endpoint.builder().host(host).port(port).build())
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
  void testBuildMongoUsingDefaultKeys() {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(
                buildConfigMapUsingDefaultKeysForMongo(), TYPE_KEY)
            .extract()
            .connectionConfig();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.MONGO)
            .addEndpoint(Endpoint.builder().host(host).port(port).build())
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
            .replicaSet(replicaSet)
            .aggregationPipelineMode(aggregatePipelineMode)
            .dataFreshness(dataFreshness)
            .queryTimeout(queryTimeout)
            .build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildPostgresUsingDefaultKeys() {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(
                buildConfigMapWithDefaultKeysForPostgres(), TYPE_KEY)
            .extract()
            .connectionConfig();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.POSTGRES)
            .addEndpoint(Endpoint.builder().host(host).port(port).build())
            .database(database)
            .credentials(ConnectionCredentials.builder().username(user).password(password).build())
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
  void testBuildPostgresUsingDefaultKeysAndCustomParams() {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(
                buildConfigMapWithDefaultKeysAndCustomParamsForPostgres(), TYPE_KEY)
            .extract()
            .connectionConfig();
    final ConnectionConfig expected =
        ConnectionConfig.builder()
            .type(DatabaseType.POSTGRES)
            .addEndpoint(Endpoint.builder().host(host).port(port).build())
            .database(database)
            .credentials(ConnectionCredentials.builder().username(user).password(password).build())
            .applicationName(appName)
            .connectionPoolConfig(
                ConnectionPoolConfig.builder()
                    .maxConnections(maxConnections)
                    .connectionAccessTimeout(accessTimeout)
                    .connectionSurrenderTimeout(surrenderTimeout)
                    .build())
            .customParameter("flatStructureCollection", "earth")
            .build();

    assertEquals(expected, config);
  }

  @Test
  void testBuildPostgresWithCollectionConfigs() {
    final ConnectionConfig config =
        TypesafeConfigDatastoreConfigExtractor.from(
                buildConfigMapWithCollectionConfigsForPostgres(), TYPE_KEY)
            .extract()
            .connectionConfig();

    final org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig
        postgresConfig =
            (org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig)
                config;

    // Verify collection config for entities_api
    var entitiesApiConfig = postgresConfig.getCollectionConfig("entities_api");
    assertEquals(true, entitiesApiConfig.isPresent());
    var timestampFields = entitiesApiConfig.get().getTimestampFields();
    assertEquals(true, timestampFields.isPresent());
    assertEquals("created_time", timestampFields.get().getCreated().get());
    assertEquals("last_updated_time", timestampFields.get().getLastUpdated().get());

    // Verify collection config for entities_domain
    var entitiesDomainConfig = postgresConfig.getCollectionConfig("entities_domain");
    assertEquals(true, entitiesDomainConfig.isPresent());
    var domainTimestampFields = entitiesDomainConfig.get().getTimestampFields();
    assertEquals(true, domainTimestampFields.isPresent());
    assertEquals("create_ts", domainTimestampFields.get().getCreated().get());
    assertEquals("update_ts", domainTimestampFields.get().getLastUpdated().get());

    // Verify non-existent collection config returns empty
    var nonExistentConfig = postgresConfig.getCollectionConfig("non_existent");
    assertEquals(false, nonExistentConfig.isPresent());
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
            entry(REPLICA_SET_KEY, replicaSet),
            entry(MAX_CONNECTIONS_KEY, maxConnections),
            entry(CONNECTION_ACCESS_TIMEOUT_KEY, accessTimeout),
            entry(CONNECTION_SURRENDER_TIMEOUT_KEY, surrenderTimeout),
            entry(AGGREGATION_PIPELINE_MODE_KEY, SORT_OPTIMIZED_IF_POSSIBLE.name()),
            entry(DATA_FRESHNESS_KEY, NEAR_REALTIME_FRESHNESS.name()),
            entry(CONNECTION_TIMEOUT_KEY, connectionTimeout),
            entry(QUERY_TIMEOUT_KEY, queryTimeout)));
  }

  private Config buildConfigMapWithDefaultKeysForPostgres() {
    return ConfigFactory.parseMap(
        Map.ofEntries(
            entry(TYPE_KEY, "postgres"),
            entry("postgres.host", host),
            entry("postgres.port", port),
            entry("postgres.database", database),
            entry("postgres.user", user),
            entry("postgres.password", password),
            entry("appName", appName),
            entry("maxPoolSize", maxConnections),
            entry("connectionAccessTimeout", accessTimeout),
            entry("connectionIdleTime", surrenderTimeout)));
  }

  private Config buildConfigMapWithDefaultKeysAndCustomParamsForPostgres() {
    return ConfigFactory.parseMap(
        Map.ofEntries(
            entry(TYPE_KEY, "postgres"),
            entry("postgres.host", host),
            entry("postgres.port", port),
            entry("postgres.database", database),
            entry("postgres.user", user),
            entry("postgres.password", password),
            entry("appName", appName),
            entry("maxPoolSize", maxConnections),
            entry("connectionAccessTimeout", accessTimeout),
            entry("connectionIdleTime", surrenderTimeout),
            entry("customParams.flatStructureCollection", "earth")));
  }

  private Config buildConfigMapUsingDefaultKeysForMongo() {
    return ConfigFactory.parseMap(
        Map.ofEntries(
            entry(TYPE_KEY, "mongo"),
            entry("mongo.host", host),
            entry("mongo.port", port),
            entry("mongo.database", database),
            entry("mongo.user", user),
            entry("mongo.password", password),
            entry("mongo.authDb", authDb),
            entry("appName", appName),
            entry("mongo.replicaSet", replicaSet),
            entry("maxPoolSize", maxConnections),
            entry("connectionAccessTimeout", accessTimeout),
            entry("connectionIdleTime", surrenderTimeout),
            entry("aggregationPipelineMode", aggregatePipelineMode.name()),
            entry("dataFreshness", dataFreshness.name()),
            entry("queryTimeout", queryTimeout)));
  }

  private Config buildPostgresConfigMapUsingEndpointsKey() {
    return ConfigFactory.parseMap(
        Map.ofEntries(
            entry(TYPE_KEY, "postgres"),
            entry(ENDPOINTS_KEY, List.of(Map.of(HOST_KEY, host, PORT_KEY, port))),
            entry(DATABASE_KEY, database),
            entry(USER_KEY, user),
            entry(PASSWORD_KEY, password),
            entry(AUTH_DB_KEY, authDb),
            entry(APP_NAME_KEY, appName),
            entry(REPLICA_SET_KEY, replicaSet),
            entry(MAX_CONNECTIONS_KEY, maxConnections),
            entry(CONNECTION_ACCESS_TIMEOUT_KEY, accessTimeout),
            entry(CONNECTION_SURRENDER_TIMEOUT_KEY, surrenderTimeout)));
  }

  private Config buildMongoConfigMapUsingEndpointsKey() {
    return ConfigFactory.parseMap(
        Map.ofEntries(
            entry(TYPE_KEY, "mongo"),
            entry(
                ENDPOINTS_KEY,
                List.of(Map.of(HOST_KEY, host1), Map.of(HOST_KEY, host2, PORT_KEY, port))),
            entry(DATABASE_KEY, database),
            entry(USER_KEY, user),
            entry(PASSWORD_KEY, password),
            entry(AUTH_DB_KEY, authDb),
            entry(APP_NAME_KEY, appName),
            entry(REPLICA_SET_KEY, replicaSet),
            entry(MAX_CONNECTIONS_KEY, maxConnections),
            entry(CONNECTION_ACCESS_TIMEOUT_KEY, accessTimeout),
            entry(CONNECTION_SURRENDER_TIMEOUT_KEY, surrenderTimeout)));
  }

  private Config buildConfigMapWithCollectionConfigsForPostgres() {
    return ConfigFactory.parseMap(
        Map.ofEntries(
            entry(TYPE_KEY, "postgres"),
            entry("postgres.host", host),
            entry("postgres.port", port),
            entry("postgres.database", database),
            entry("postgres.user", user),
            entry("postgres.password", password),
            entry("appName", appName),
            entry("maxPoolSize", maxConnections),
            entry("connectionAccessTimeout", accessTimeout),
            entry("connectionIdleTime", surrenderTimeout),
            entry(
                "postgres.collectionConfigs.entities_api.timestampFields.created", "created_time"),
            entry(
                "postgres.collectionConfigs.entities_api.timestampFields.lastUpdated",
                "last_updated_time"),
            entry(
                "postgres.collectionConfigs.entities_domain.timestampFields.created", "create_ts"),
            entry(
                "postgres.collectionConfigs.entities_domain.timestampFields.lastUpdated",
                "update_ts")));
  }
}
