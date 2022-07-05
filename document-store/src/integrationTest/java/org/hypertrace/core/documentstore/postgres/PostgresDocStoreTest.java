package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest.Operation.ADD;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.JSONDocument;
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
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

  @Test
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

  @Test
  public void whenBulkUpdatingExistingRecords_thenExpectOnlyRecordsWhoseConditionsMatchToBeUpdated()
      throws Exception {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    ObjectNode persistedObject = OBJECT_MAPPER.createObjectNode();
    persistedObject.put("foo1", "bar1");
    persistedObject.put("timestamp", 90);

    collection.create(
        new SingleValueKey("tenant-1", "testKey1"), new JSONDocument(persistedObject));

    ObjectNode updatedObject = OBJECT_MAPPER.createObjectNode();
    updatedObject.put("foo1", "bar1");
    updatedObject.put("timestamp", 110);

    List<BulkUpdateRequest> toUpdate = new ArrayList<>();
    toUpdate.add(
        new BulkUpdateRequest(
            new SingleValueKey("tenant-1", "testKey1"),
            new JSONDocument(updatedObject),
            new Filter(Op.LT, "timestamp", 100)));

    toUpdate.add(
        new BulkUpdateRequest(
            new SingleValueKey("tenant-1", "testKey2"),
            new JSONDocument(updatedObject),
            new Filter(Op.LT, "timestamp", 100)));

    BulkUpdateResult result = collection.bulkUpdate(toUpdate);
    Assertions.assertEquals(1, result.getUpdatedCount());

    Query query = new Query();
    query.setFilter(
        new Filter(Op.EQ, "_id", new SingleValueKey("tenant-1", "testKey1").toString()));
    Iterator<Document> it = collection.search(query);
    JsonNode root = OBJECT_MAPPER.readTree(it.next().toJson());
    Long timestamp = root.findValue("timestamp").asLong();
    Assertions.assertEquals(110, timestamp);
  }

  @Test
  public void test_bulkOperationOnArrayValue_addOperation() throws Exception {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Key key1 = new SingleValueKey("default", "testKey1");
    Document key1InsertedDocument =
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of(
                "attributes",
                Map.of(
                    "name",
                    "testKey1",
                    "labels",
                    ImmutablePair.of(
                        "valueList",
                        ImmutablePair.of(
                            "values",
                            List.of(ImmutablePair.of("value", Map.of("string", "Label1"))))))));
    Document key1ExpectedDocument =
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of(
                "attributes",
                Map.of(
                    "name",
                    "testKey1",
                    "labels",
                    ImmutablePair.of(
                        "valueList",
                        ImmutablePair.of(
                            "values",
                            List.of(
                                ImmutablePair.of("value", Map.of("string", "Label1")),
                                ImmutablePair.of("value", Map.of("string", "Label2")),
                                ImmutablePair.of("value", Map.of("string", "Label3"))))))));
    collection.upsert(key1, key1InsertedDocument);

    Key key2 = new SingleValueKey("default", "testKey2");
    Document key2InsertedDocument =
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of(
                "attributes",
                Map.of(
                    "name",
                    "testKey2",
                    "labels",
                    ImmutablePair.of(
                        "valueList",
                        ImmutablePair.of(
                            "values",
                            List.of(ImmutablePair.of("value", Map.of("string", "Label2"))))))));
    Document key2ExpectedDocument =
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of(
                "attributes",
                Map.of(
                    "name",
                    "testKey2",
                    "labels",
                    ImmutablePair.of(
                        "valueList",
                        ImmutablePair.of(
                            "values",
                            List.of(
                                ImmutablePair.of("value", Map.of("string", "Label2")),
                                ImmutablePair.of("value", Map.of("string", "Label3"))))))));
    collection.upsert(key2, key2InsertedDocument);

    Key key3 = new SingleValueKey("default", "testKey3");
    Document key3InsertedDocument =
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("attributes", Map.of("name", "testKey3")));
    Document key3ExpectedDocument =
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of(
                "attributes",
                Map.of(
                    "name",
                    "testKey3",
                    "labels",
                    ImmutablePair.of(
                        "valueList",
                        ImmutablePair.of(
                            "values",
                            List.of(
                                ImmutablePair.of("value", Map.of("string", "Label2")),
                                ImmutablePair.of("value", Map.of("string", "Label3"))))))));
    collection.upsert(key3, key3InsertedDocument);

    Key key4 = new SingleValueKey("default", "testKey4");
    Document key4InsertedDocument =
        Utils.createDocument(
            ImmutablePair.of("id", "testKey4"),
            ImmutablePair.of(
                "attributes",
                Map.of(
                    "name",
                    "testKey4",
                    "labels",
                    ImmutablePair.of(
                        "valueList",
                        ImmutablePair.of(
                            "values",
                            List.of(
                                ImmutablePair.of("value", Map.of("string", "Label1")),
                                ImmutablePair.of("value", Map.of("string", "Label2")),
                                ImmutablePair.of("value", Map.of("string", "Label3"))))))));
    Document key4ExpectedDocument =
        Utils.createDocument(
            ImmutablePair.of("id", "testKey4"),
            ImmutablePair.of(
                "attributes",
                Map.of(
                    "name",
                    "testKey4",
                    "labels",
                    ImmutablePair.of(
                        "valueList",
                        ImmutablePair.of(
                            "values",
                            List.of(
                                ImmutablePair.of("value", Map.of("string", "Label1")),
                                ImmutablePair.of("value", Map.of("string", "Label2")),
                                ImmutablePair.of("value", Map.of("string", "Label3"))))))));
    collection.upsert(key4, key4InsertedDocument);

    Document label2Document =
        Utils.createDocument(ImmutablePair.of("value", Map.of("string", "Label2")));
    Document label3Document =
        Utils.createDocument(ImmutablePair.of("value", Map.of("string", "Label3")));
    List<Document> subDocuments = List.of(label2Document, label3Document);

    BulkArrayValueUpdateRequest bulkArrayValueUpdateRequest =
        new BulkArrayValueUpdateRequest(
            Set.of(key1, key2, key3, key4),
            "attributes.labels.valueList.values",
            ADD,
            subDocuments);
    BulkUpdateResult bulkUpdateResult =
        collection.bulkOperationOnArrayValue(bulkArrayValueUpdateRequest);
    assertEquals(4, bulkUpdateResult.getUpdatedCount());
  }
}
