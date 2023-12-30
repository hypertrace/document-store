package org.hypertrace.core.documentstore.model.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;
import org.junit.jupiter.api.Test;

class MongoConnectionConfigTest {

  @Test
  void testAllDefaults() {
    final MongoConnectionConfig mongoConnectionConfig =
        (MongoConnectionConfig) ConnectionConfig.builder().type(DatabaseType.MONGO).build();

    final MongoClientSettings expected =
        MongoClientSettings.builder()
            .applicationName("document-store")
            .retryWrites(true)
            .applyToClusterSettings(
                builder -> builder.requiredReplicaSetName(null).hosts(List.of(new ServerAddress())))
            .applyToConnectionPoolSettings(
                builder ->
                    builder
                        .maxConnecting(16)
                        .maxWaitTime(10, TimeUnit.SECONDS)
                        .maxConnectionIdleTime(5, TimeUnit.MINUTES))
            .build();

    final MongoClientSettings actual = mongoConnectionConfig.toSettings();
    assertEquals(expected, actual);
  }

  @Test
  void testAllAssigned() {
    final String applicationName = "my-Mars-app";
    final String authDb = "auth_db";
    final String username = "user-from-Mars";
    final String password = "encrypted-message-from-Mars";

    final int maxPoolConnections = 1;
    final Duration maxWaitTime = Duration.ofSeconds(5);
    final Duration maxIdleTime = Duration.ofSeconds(30);

    final MongoConnectionConfig mongoConnectionConfig =
        (MongoConnectionConfig)
            ConnectionConfig.builder()
                .type(DatabaseType.MONGO)
                .addEndpoint(Endpoint.builder().host("the.red.giant.com").port(37017).build())
                .database("test_db")
                .credentials(
                    ConnectionCredentials.builder()
                        .authDatabase(authDb)
                        .username(username)
                        .password(password)
                        .build())
                .applicationName(applicationName)
                .connectionPoolConfig(
                    ConnectionPoolConfig.builder()
                        .maxConnections(maxPoolConnections)
                        .connectionAccessTimeout(maxWaitTime)
                        .connectionSurrenderTimeout(maxIdleTime)
                        .build())
                .build();

    final MongoClientSettings expected =
        MongoClientSettings.builder()
            .applicationName(applicationName)
            .retryWrites(true)
            .credential(MongoCredential.createCredential(username, authDb, password.toCharArray()))
            .applyToClusterSettings(
                builder ->
                    builder
                        .requiredReplicaSetName(null)
                        .hosts(List.of(new ServerAddress("the.red.giant.com", 37017))))
            .applyToConnectionPoolSettings(
                builder ->
                    builder
                        .maxConnecting(maxPoolConnections)
                        .maxWaitTime(maxWaitTime.toSeconds(), TimeUnit.SECONDS)
                        .maxConnectionIdleTime(maxIdleTime.toSeconds(), TimeUnit.SECONDS))
            .build();

    final MongoClientSettings actual = mongoConnectionConfig.toSettings();
    assertEquals(expected, actual);
  }

  @Test
  void testMissingAuthDb_shouldUseTheSameDatabaseUsedForConnecting() {
    final String applicationName = "my-Mars-app";
    final String username = "user-from-Mars";
    final String password = "encrypted-message-from-Mars";
    final String database = "test_db";
    final String replicaSetName = "replica-set";

    final MongoConnectionConfig mongoConnectionConfig =
        (MongoConnectionConfig)
            ConnectionConfig.builder()
                .type(DatabaseType.MONGO)
                .addEndpoint(Endpoint.builder().host("the.red.giant.com").port(37017).build())
                .database(database)
                .replicaSet(replicaSetName)
                .credentials(
                    ConnectionCredentials.builder().username(username).password(password).build())
                .applicationName(applicationName)
                .build();

    final MongoClientSettings expected =
        MongoClientSettings.builder()
            .applicationName(applicationName)
            .retryWrites(true)
            .credential(
                MongoCredential.createCredential(username, database, password.toCharArray()))
            .applyToClusterSettings(
                builder ->
                    builder
                        .requiredReplicaSetName(replicaSetName)
                        .hosts(List.of(new ServerAddress("the.red.giant.com", 37017))))
            .applyToConnectionPoolSettings(
                builder ->
                    builder
                        .maxConnecting(16)
                        .maxWaitTime(10, TimeUnit.SECONDS)
                        .maxConnectionIdleTime(5, TimeUnit.MINUTES))
            .build();

    final MongoClientSettings actual = mongoConnectionConfig.toSettings();
    assertEquals(expected, actual);
  }

  @Test
  void testDefaultCredentials_shouldNotSetCredentials() {
    final String applicationName = "my-Mars-app";

    final MongoConnectionConfig mongoConnectionConfig =
        (MongoConnectionConfig)
            ConnectionConfig.builder()
                .type(DatabaseType.MONGO)
                .addEndpoint(Endpoint.builder().host("the.red.giant.com").port(37017).build())
                .database("test_db")
                .credentials(ConnectionCredentials.builder().build())
                .applicationName(applicationName)
                .build();

    final MongoClientSettings expected =
        MongoClientSettings.builder()
            .applicationName(applicationName)
            .retryWrites(true)
            .applyToClusterSettings(
                builder ->
                    builder
                        .requiredReplicaSetName(null)
                        .hosts(List.of(new ServerAddress("the.red.giant.com", 37017))))
            .applyToConnectionPoolSettings(
                builder ->
                    builder
                        .maxConnecting(16)
                        .maxWaitTime(10, TimeUnit.SECONDS)
                        .maxConnectionIdleTime(5, TimeUnit.MINUTES))
            .build();

    final MongoClientSettings actual = mongoConnectionConfig.toSettings();
    assertEquals(expected, actual);
  }
}
