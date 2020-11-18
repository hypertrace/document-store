package org.hypertrace.core.documentstore.postgres;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

public class PostgresDatastoreTest {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDatastoreTest.class);
  
  
  @Test
  public void testInit() {
    PostgresDatastore datastore = new PostgresDatastore();
    Properties properties = new Properties();
    String url = "jdbc:postgresql://localhost:5432/";
    String user = "postgres";
    String password = "postgres";
    
    properties.put("url", url);
    properties.put("user", user);
    properties.put("password", password);
    Config config = ConfigFactory.parseProperties(properties);
    datastore.init(config);
    
    try {
      DatabaseMetaData metaData = datastore.getPostgresClient().getMetaData();
      Assertions.assertEquals(metaData.getURL(), url);
      Assertions.assertEquals(metaData.getUserName(), user);
    } catch (SQLException e) {
      LOGGER.error("Exception executing init test with user and password");
    }
    
  }
  
  @Test
  public void testInitWithDatabase() {
    PostgresDatastore datastore = new PostgresDatastore();
    Properties properties = new Properties();
    String url = "jdbc:postgresql://localhost:5432/";
    String user = "postgres";
    String password = "postgres";
    String database = "postgres";
    
    properties.put("url", url);
    properties.put("user", user);
    properties.put("password", password);
    properties.put("database", database);
    Config config = ConfigFactory.parseProperties(properties);
    datastore.init(config);
    
    try {
      DatabaseMetaData metaData = datastore.getPostgresClient().getMetaData();
      Assertions.assertEquals(metaData.getURL(), url + database);
      Assertions.assertEquals(metaData.getUserName(), user);
    } catch (SQLException e) {
      LOGGER.error("Exception executing init test with user and password");
    }
    
  }
  
}
