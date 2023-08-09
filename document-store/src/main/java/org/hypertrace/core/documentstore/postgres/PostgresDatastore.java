package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.PostgresCollection.CREATED_AT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.UPDATED_AT;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.metric.MetricStore;
import org.hypertrace.core.documentstore.metric.postgres.PostgresMetricStore;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides {@link Datastore} implementation for Postgres DB. */
public class PostgresDatastore implements Datastore {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDatastore.class);

  private final PostgresClient client;
  private final String database;
  private final MetricStore metricStore;

  public PostgresDatastore(@NonNull final DatastoreConfig datastoreConfig) {
    final ConnectionConfig connectionConfig = datastoreConfig.connectionConfig();

    if (!(connectionConfig instanceof PostgresConnectionConfig)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot pass %s as config to %s",
              connectionConfig.getClass().getSimpleName(), this.getClass().getSimpleName()));
    }

    final PostgresConnectionConfig postgresConnectionConfig =
        (PostgresConnectionConfig) connectionConfig;

    try {
      DriverManager.registerDriver(new org.postgresql.Driver());

      client = new PostgresClient(postgresConnectionConfig);
      database = connectionConfig.database();
      metricStore = new PostgresMetricStore(this, postgresConnectionConfig);
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

  @Override
  public MetricStore getMetricStore() {
    return metricStore;
  }

  public Connection getPostgresClient() {
    return client.getConnection();
  }
}
