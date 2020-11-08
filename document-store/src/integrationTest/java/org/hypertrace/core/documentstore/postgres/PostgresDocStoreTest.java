package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.util.HashMap;
import java.util.Map;

public class PostgresDocStoreTest {
  private static final String COLLECTION_NAME = "mytest";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  private static Datastore datastore;
  
  
  @BeforeAll
  public static void init() {
    
    DatastoreProvider.register("POSTGRES", PostgresDatastore.class);
    
    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.putIfAbsent("url", "jdbc:postgresql://localhost:5432");
    postgresConfig.putIfAbsent("user", "postgres");
    postgresConfig.putIfAbsent("password", "postgres");
    Config config = ConfigFactory.parseMap(postgresConfig);
    
    datastore = DatastoreProvider.getDatastore("Postgres", config);
    System.out.println(datastore.listCollections());
  }
  
  @AfterEach
  public void cleanup() {
    datastore.deleteCollection(COLLECTION_NAME);
    datastore.createCollection(COLLECTION_NAME, null);
  }
  
  
}
