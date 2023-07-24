package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.PostgresCollection.CREATED_AT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.UPDATED_AT;

import com.typesafe.config.Config;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.DatabaseType;
import org.hypertrace.core.documentstore.model.config.TypesafeConfigConnectionConfigExtractor;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides {@link Datastore} implementation for Postgres DB. */
public class PostgresDatastore implements Datastore {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDatastore.class);

  private PostgresClient client;
  private String database;

  @Override
  public boolean init(Config config) {
    try {
      final PostgresConnectionConfig connectionConfig =
          (PostgresConnectionConfig)
              TypesafeConfigConnectionConfigExtractor.from(config, DatabaseType.POSTGRES)
                  .hostKey("host")
                  .portKey("port")
                  .usernameKey("user")
                  .passwordKey("password")
                  .databaseKey("database")
                  .applicationNameKey("applicationName")
                  .poolMaxConnectionsKey("connectionPool.maxConnections")
                  .poolConnectionAccessTimeoutKey("connectionPool.maxWaitTime")
                  .poolConnectionSurrenderTimeoutKey("connectionPool.removeAbandonedTimeout")
                  .extract();

      init(
          new PostgresConnectionConfig(
              connectionConfig.host(),
              connectionConfig.port(),
              connectionConfig.database(),
              connectionConfig.credentials(),
              connectionConfig.applicationName(),
              connectionConfig.connectionPoolConfig()) {
            @Override
            public String toConnectionString() {
              return config.hasPath("url")
                  ? config.getString("url") + connectionConfig.database()
                  : super.toConnectionString();
            }
          });
      return true;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unable to instantiate PostgresClient with config:%s", config), e);
    }
  }

  @Override
  public void init(final ConnectionConfig connectionConfig) {
    try {
      DriverManager.registerDriver(new org.postgresql.Driver());

      client = new PostgresClient(connectionConfig);
      database = connectionConfig.database();
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unable to instantiate PostgresClient with config:%s", connectionConfig),
          e);
    } catch (final SQLException e) {
      throw new RuntimeException("PostgresClient SQLException", e);
    }
  }

  /** @return Returns Tables for a particular database */
  @Override
  public Set<String> listCollections() {
    Set<String> collections = new HashSet<>();
    try {
      DatabaseMetaData metaData = client.getConnection().getMetaData();
      ResultSet tables = metaData.getTables(null, null, "%", new String[] {"TABLE"});
      while (tables.next()) {
        collections.add(database + "." + tables.getString("TABLE_NAME"));
      }
    } catch (SQLException e) {
      LOGGER.error("Exception getting postgres metadata");
    }
    return collections;
  }

  @Override
  public boolean createCollection(String collectionName, Map<String, String> options) {
    String createTableSQL =
        String.format(
            "CREATE TABLE %s ("
                + "%s TEXT PRIMARY KEY,"
                + "%s jsonb NOT NULL,"
                + "%s TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
                + "%s TIMESTAMPTZ NOT NULL DEFAULT NOW()"
                + ");",
            collectionName, ID, DOCUMENT, CREATED_AT, UPDATED_AT);
    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(createTableSQL)) {
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Exception creating table name: {}", collectionName);
      return false;
    }
    return true;
  }

  @Override
  public boolean deleteCollection(String collectionName) {
    String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", collectionName);
    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(dropTableSQL)) {
      int result = preparedStatement.executeUpdate();
      return result >= 0;
    } catch (SQLException e) {
      LOGGER.error("Exception deleting table name: {}", collectionName);
    }
    return false;
  }

  @Override
  public Collection getCollection(String collectionName) {
    Set<String> tables = listCollections();
    if (!tables.contains(collectionName)) {
      createCollection(collectionName, null);
    }
    return new PostgresCollection(client, collectionName);
  }

  @Override
  public boolean healthCheck() {
    String healtchCheckSQL = "SELECT 1;";
    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(healtchCheckSQL)) {
      return preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.error("Exception executing health check");
    }
    return false;
  }

  public Connection getPostgresClient() {
    return client.getConnection();
  }
}
