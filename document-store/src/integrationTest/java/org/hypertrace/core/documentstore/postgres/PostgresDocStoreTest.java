package org.hypertrace.core.documentstore.postgres;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class PostgresDocStoreTest {

  private static final String COLLECTION_NAME = "mytest";

  private static GenericContainer<?> postgres;
  private static Datastore datastore;
  private static String connectionUrl;

  @BeforeAll
  public static void init() {
    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgres.start();

    connectionUrl = String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));
    DatastoreProvider.register("POSTGRES", PostgresDatastore.class);

    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.putIfAbsent("url", connectionUrl);
    postgresConfig.putIfAbsent("user", "postgres");
    postgresConfig.putIfAbsent("password", "postgres");
    Config config = ConfigFactory.parseMap(postgresConfig);

    datastore = DatastoreProvider.getDatastore("Postgres", config);
    System.out.println(datastore.listCollections());
  }

  @BeforeEach
  public void setUp() {
    datastore.deleteCollection(COLLECTION_NAME);
    datastore.createCollection(COLLECTION_NAME, null);
  }

  @AfterAll
  public static void shutdown() {
    postgres.stop();
  }

  @Test
  public void testInitWithDatabase() {
    PostgresDatastore datastore = new PostgresDatastore();
    Properties properties = new Properties();
    String user = "postgres";
    String password = "postgres";
    String database = "postgres";

    properties.put("url", connectionUrl);
    properties.put("user", user);
    properties.put("password", password);
    properties.put("database", database);
    Config config = ConfigFactory.parseProperties(properties);
    datastore.init(config);

    try {
      DatabaseMetaData metaData = datastore.getPostgresClient().getMetaData();
      Assertions.assertEquals(metaData.getURL(), connectionUrl + database);
      Assertions.assertEquals(metaData.getUserName(), user);
    } catch (SQLException e) {
      System.out.println("Exception executing init test with user and password");
      Assertions.fail();
    }
  }

  public void testUpsertAndReturn() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Document document = Utils.createDocument("foo1", "bar1");
    Document resultDocument =
        collection.upsertAndReturn(new SingleValueKey("default", "testKey"), document);

    Assertions.assertEquals(document.toJson(), resultDocument.toJson());
  }

  @Test
  public void testBulkUpsertAndReturn() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Document> bulkMap = new HashMap<>();
    bulkMap.put(new SingleValueKey("default", "testKey1"), Utils.createDocument("name", "Bob"));
    bulkMap.put(new SingleValueKey("default", "testKey2"), Utils.createDocument("name", "Alice"));
    bulkMap.put(new SingleValueKey("default", "testKey3"), Utils.createDocument("name", "Alice"));
    bulkMap.put(new SingleValueKey("default", "testKey4"), Utils.createDocument("name", "Bob"));
    bulkMap.put(new SingleValueKey("default", "testKey5"), Utils.createDocument("name", "Alice"));
    bulkMap.put(
        new SingleValueKey("default", "testKey6"),
        Utils.createDocument("email", "bob@example.com"));

    Iterator<Document> iterator = collection.bulkUpsertAndReturnOlderDocuments(bulkMap);
    // Initially there shouldn't be any documents.
    Assertions.assertFalse(iterator.hasNext());

    // The operation should be idempotent, so go ahead and try again.
    iterator = collection.bulkUpsertAndReturnOlderDocuments(bulkMap);
    List<Document> documents = new ArrayList<>();
    while (iterator.hasNext()) {
      documents.add(iterator.next());
    }
    Assertions.assertEquals(6, documents.size());

    {
      // empty query returns all the documents
      Query query = new Query();
      Assertions.assertEquals(6, collection.total(query));
    }

    {
      Query query = new Query();
      query.setFilter(Filter.eq("name", "Bob"));
      Assertions.assertEquals(2, collection.total(query));
    }

    {
      // limit should not affect the total
      Query query = new Query();
      query.setFilter(Filter.eq("name", "Bob"));
      query.setLimit(1);
      Assertions.assertEquals(2, collection.total(query));
    }
  }

  @Test
  public void testDrop() {
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    Assertions.assertTrue(datastore.listCollections().contains("postgres." + COLLECTION_NAME));
    collection.drop();
    Assertions.assertFalse(datastore.listCollections().contains("postgres." + COLLECTION_NAME));
  }
}
