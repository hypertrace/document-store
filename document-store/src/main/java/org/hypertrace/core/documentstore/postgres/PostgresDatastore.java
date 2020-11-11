package org.hypertrace.core.documentstore.postgres;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PostgresDatastore implements Datastore {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDatastore.class);
  
  private Connection client;
  // Specifies whether document will be stored in json/jsonb format.
  private String columnType = "json";
  private String database = "postgres";
  
  @Override
  public boolean init(Config config) {
    try {
      DriverManager.registerDriver(new org.postgresql.Driver());
      String url = config.getString("url");
      if (config.hasPath("type")) {
        this.columnType = config.getString("type");
      }
      // Database needs to be created before initializing connection
      if (config.hasPath("database")) {
        this.database = config.getString("database");
        url = url + database;
      }
      if (config.hasPath("user") && config.hasPath("password")) {
        client = DriverManager.getConnection(url, config.getString("user"), config.getString("password"));
      } else {
        client = DriverManager.getConnection(url);
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
        String.format("Unable to instantiate PostgresClient with config:%s", config), e);
    } catch (SQLException e) {
      throw new RuntimeException("PostgresClient SQLException", e);
    }
    return true;
  }
  
  /**
   * @return Returns Tables for a particular database
   */
  @Override
  public Set<String> listCollections() {
    Set<String> collections = new HashSet<>();
    try {
      DatabaseMetaData metaData = client.getMetaData();
      ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
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
    String createTableSQL = String.format("CREATE TABLE %s (" +
      "id TEXT PRIMARY KEY," +
      "document %s NOT NULL," +
      "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()," +
      "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" +
      ");", collectionName, columnType);
    try {
      PreparedStatement preparedStatement = client.prepareStatement(createTableSQL);
      int result = preparedStatement.executeUpdate();
      preparedStatement.close();
      return result >= 0;
    } catch (SQLException e) {
      LOGGER.error("Exception creating table name: {}", collectionName);
    }
    return false;
  }
  
  @Override
  public boolean deleteCollection(String collectionName) {
    String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", collectionName);
    try {
      PreparedStatement preparedStatement = client.prepareStatement(dropTableSQL);
      int result = preparedStatement.executeUpdate();
      preparedStatement.close();
      return result >= 0;
    } catch (SQLException e) {
      LOGGER.error("Exception deleting table name: {}", collectionName);
    }
    return false;
  }
  
  @Override
  public Collection getCollection(String collectionName) {
    return new PostgresCollection(client, collectionName, columnType);
  }
  
  @Override
  public boolean healthCheck() {
    String healtchCheckSQL = "SELECT 1;";
    try {
      PreparedStatement preparedStatement = client.prepareStatement(healtchCheckSQL);
      return preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.error("Exception executing health check");
    }
    
    return false;
  }
  
  @VisibleForTesting
  Connection getPostgresClient() {
    return client;
  }
}
