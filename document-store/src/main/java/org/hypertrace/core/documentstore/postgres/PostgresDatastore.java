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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides {@link Datastore} implementation for Postgres DB. */
public class PostgresDatastore implements Datastore {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDatastore.class);

  private static final String DEFAULT_USER = "postgres";
  private static final String DEFAULT_PASSWORD = "postgres";
  private static final String DEFAULT_DB_NAME = "postgres";
  private static final int DEFAULT_MAX_CONNECTIONS = 5;

  private PostgresClient client;
  private String database;

  @Override
  public boolean init(Config config) {
    try {
      DriverManager.registerDriver(new org.postgresql.Driver());
      this.database = config.hasPath("database") ? config.getString("database") : DEFAULT_DB_NAME;

      String url;
      if (config.hasPath("url")) {
        url = config.getString("url");
      } else {
        String hostName = config.getString("host");
        int port = config.getInt("port");
        url = String.format("jdbc:postgresql://%s:%s/", hostName, port);
      }

      String user = config.hasPath("user") ? config.getString("user") : DEFAULT_USER;
      String password =
          config.hasPath("password") ? config.getString("password") : DEFAULT_PASSWORD;
      int maxConnections =
          config.hasPath("maxConnections")
              ? config.getInt("maxConnections")
              : DEFAULT_MAX_CONNECTIONS;

      String finalUrl = url + this.database;
      client = new PostgresClient(finalUrl, user, password, maxConnections);

    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unable to instantiate PostgresClient with config:%s", config), e);
    } catch (SQLException e) {
      throw new RuntimeException("PostgresClient SQLException", e);
    }
    return true;
  }

  /** @return Returns Tables for a particular database */
  @Override
  public Set<String> listCollections() {
    Set<String> collections = new HashSet<>();
    try (Connection connection = client.getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
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
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(createTableSQL)) {
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
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(dropTableSQL)) {
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
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(healtchCheckSQL)) {
      return preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.error("Exception executing health check");
    }
    return false;
  }

  public PostgresClient getPostgresClient() {
    return client;
  }
}
