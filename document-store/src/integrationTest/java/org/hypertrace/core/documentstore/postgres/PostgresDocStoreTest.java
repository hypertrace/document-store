package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
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
    Properties properties = new Properties();
    String user = "postgres";
    String password = "postgres";
    String database = "postgres";

    properties.put("url", connectionUrl);
    properties.put("user", user);
    properties.put("password", password);
    properties.put("database", database);
    Config config = ConfigFactory.parseProperties(properties);
    PostgresDatastore datastore =
        (PostgresDatastore) DatastoreProvider.getDatastore("postgres", config);

    try {
      DatabaseMetaData metaData = datastore.getPostgresClient().getMetaData();
      assertEquals(connectionUrl + database, metaData.getURL());
      assertEquals(user, metaData.getUserName());
    } catch (SQLException e) {
      System.out.println("Exception executing init test with user and password");
      Assertions.fail();
    }
  }

  @Test
  public void testUpsertAndReturn() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Document document = Utils.createDocument("foo1", "bar1");
    Document resultDocument =
        collection.upsertAndReturn(new SingleValueKey("default", "testKey"), document);

    assertEquals(document.toJson(), resultDocument.toJson());
  }

  @Test
  public void test_getJsonNodeAtPath() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode labelNode = objectMapper.createObjectNode();
    labelNode.put("string", "Label2");

    ObjectNode valueNode = objectMapper.createObjectNode();
    valueNode.set("value", labelNode);

    ArrayNode valuesNode = objectMapper.createArrayNode();
    valuesNode.add(valueNode);

    ObjectNode valueListNode = objectMapper.createObjectNode();
    valueListNode.set("values", valuesNode);

    ObjectNode labelsNode = objectMapper.createObjectNode();
    labelsNode.set("valueList", valueListNode);

    ObjectNode attributesNode = objectMapper.createObjectNode();
    attributesNode.put("name", "testKey2");
    attributesNode.set("labels", labelsNode);

    ObjectNode rootNode = objectMapper.createObjectNode();
    rootNode.put("id", "testKey2");
    rootNode.set("attributes", attributesNode);
    rootNode.put("created_at", "2022-07-12 17:46:03.750437");
    rootNode.put("updated_at", "2022-07-12 17:46:03.750437");

    String path1 = "attributes.labels.valueList.values";
    String outputNode1 = valuesNode.toString();
    JsonNode expectedRootNode1 = objectMapper.readTree(outputNode1);

    String path2 = "attributes.labels.attrNotPresent.values";
    String outputNode2 = "[]";
    JsonNode expectedRootNode2 = objectMapper.readTree(outputNode2);
    PostgresCollection collection = (PostgresCollection) datastore.getCollection(COLLECTION_NAME);
    try {
      assertEquals(collection.getJsonNodeAtPath(path1, rootNode, true), expectedRootNode1);
      assertEquals(collection.getJsonNodeAtPath(path2, rootNode, true), expectedRootNode2);
      assertEquals(collection.getJsonNodeAtPath(null, rootNode, true), rootNode);
      assertEquals(collection.getJsonNodeAtPath(path1, rootNode, false), expectedRootNode1);
      assertEquals(collection.getJsonNodeAtPath(path2, rootNode, false), expectedRootNode2);
      assertEquals(collection.getJsonNodeAtPath(null, rootNode, false), rootNode);
    } catch (Exception e) {
      System.out.println("Created path is not right");
      Assertions.fail();
    }
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
    assertEquals(6, documents.size());

    {
      // empty query returns all the documents
      Query query = new Query();
      assertEquals(6, collection.total(query));
    }

    {
      Query query = new Query();
      query.setFilter(Filter.eq("name", "Bob"));
      assertEquals(2, collection.total(query));
    }

    {
      // limit should not affect the total
      Query query = new Query();
      query.setFilter(Filter.eq("name", "Bob"));
      query.setLimit(1);
      assertEquals(2, collection.total(query));
    }
  }

  @Test
  public void testDrop() {
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    assertTrue(datastore.listCollections().contains("postgres." + COLLECTION_NAME));
    collection.drop();
    Assertions.assertFalse(datastore.listCollections().contains("postgres." + COLLECTION_NAME));
  }

  @Test
  void testBasicSchemaTableOps() throws IOException {
    Collection collection = datastore.getCollection("schema.myTest");
    assertTrue(datastore.listCollections().contains("postgres.schema.myTest"));
    Document document = Utils.createDocument("foo1", "bar1");
    Document resultDocument =
        collection.createOrReplaceAndReturn(new SingleValueKey("default", "testKey"), document);
    assertEquals(
        "bar1", new ObjectMapper().readTree(resultDocument.toJson()).get("foo1").textValue());

    // Going to the same table without the schema should not get any data
    assertFalse(
        datastore
            .getCollection(COLLECTION_NAME)
            .aggregate(
                org.hypertrace.core.documentstore.query.Query.builder()
                    .addSelection(IdentifierExpression.of("foo1"))
                    .build())
            .hasNext());

    assertTrue(datastore.deleteCollection("schema.myTest"));
    assertFalse(datastore.listCollections().contains("postgres.schema.myTest"));
  }
}
