package org.hypertrace.core.documentstore.model.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;
import org.junit.jupiter.api.Test;

class MongoConnectionConfigTest {

  @Test
  void testAllDefaults() {
    final MongoConnectionConfig mongoConnectionConfig =
        (MongoConnectionConfig) ConnectionConfig.builder().type(DatabaseType.MONGO).build();

    final MongoClientSettings expected =
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString("mongodb://localhost:27017/default_db"))
            .applicationName("document-store")
            .retryWrites(true)
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

    final MongoConnectionConfig mongoConnectionConfig =
        (MongoConnectionConfig)
            ConnectionConfig.builder()
                .type(DatabaseType.MONGO)
                .host("the.red.giant.com")
                .port(31707)
                .database("test_db")
                .credentials(
                    ConnectionCredentials.builder()
                        .authDatabase(authDb)
                        .username(username)
                        .password(password)
                        .build())
                .applicationName(applicationName)
                .build();

    final MongoClientSettings expected =
        MongoClientSettings.builder()
            .applyConnectionString(
                new ConnectionString("mongodb://the.red.giant.com:31707/test_db"))
            .applicationName(applicationName)
            .retryWrites(true)
            .credential(MongoCredential.createCredential(username, authDb, password.toCharArray()))
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

    final MongoConnectionConfig mongoConnectionConfig =
        (MongoConnectionConfig)
            ConnectionConfig.builder()
                .type(DatabaseType.MONGO)
                .host("the.red.giant.com")
                .port(31707)
                .database(database)
                .credentials(
                    ConnectionCredentials.builder().username(username).password(password).build())
                .applicationName(applicationName)
                .build();

    final MongoClientSettings expected =
        MongoClientSettings.builder()
            .applyConnectionString(
                new ConnectionString("mongodb://the.red.giant.com:31707/test_db"))
            .applicationName(applicationName)
            .retryWrites(true)
            .credential(
                MongoCredential.createCredential(username, database, password.toCharArray()))
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
                .host("the.red.giant.com")
                .port(31707)
                .database("test_db")
                .credentials(ConnectionCredentials.builder().build())
                .applicationName(applicationName)
                .build();

    final MongoClientSettings expected =
        MongoClientSettings.builder()
            .applyConnectionString(
                new ConnectionString("mongodb://the.red.giant.com:31707/test_db"))
            .applicationName(applicationName)
            .retryWrites(true)
            .build();

    final MongoClientSettings actual = mongoConnectionConfig.toSettings();
    assertEquals(expected, actual);
  }
}
