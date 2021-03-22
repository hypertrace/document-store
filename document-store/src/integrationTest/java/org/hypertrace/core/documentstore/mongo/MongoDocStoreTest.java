package org.hypertrace.core.documentstore.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Integration tests for the MongoDB doc store */
@Testcontainers
public class MongoDocStoreTest {

  private static final String COLLECTION_NAME = "myTest";
  private static Datastore datastore;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static GenericContainer<?> mongo;

  /*
   * These 3 fields should be automatically created when upserting a doc.
   * There are downstream services that depends on this. The test should verify that
   * the string is not changed.
   */
  private static final String LAST_UPDATE_TIME_KEY = "_lastUpdateTime";
  private static final String LAST_UPDATED_TIME_KEY = "lastUpdatedTime";
  private static final String LAST_CREATED_TIME_KEY = "createdTime";

  @BeforeAll
  public static void init() {
    mongo =
        new GenericContainer<>(DockerImageName.parse("mongo:4.4.0"))
            .withExposedPorts(27017)
            .waitingFor(Wait.forListeningPort());
    mongo.start();

    DatastoreProvider.register("MONGO", MongoDatastore.class);

    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.putIfAbsent("host", "localhost");
    mongoConfig.putIfAbsent("port", mongo.getMappedPort(27017).toString());
    Config config = ConfigFactory.parseMap(mongoConfig);

    datastore = DatastoreProvider.getDatastore("Mongo", config);
    System.out.println(datastore.listCollections());
  }

  @AfterEach
  public void cleanup() {
    datastore.deleteCollection(COLLECTION_NAME);
    datastore.createCollection(COLLECTION_NAME, null);
  }

  @AfterAll
  public static void shutdown() {
    mongo.stop();
  }

  @Test
  public void testCollections() {
    for (String collection : datastore.listCollections()) {
      datastore.deleteCollection(collection);
    }

    assertTrue(datastore.createCollection(COLLECTION_NAME, null));

    // Retry again and you should still receive true.
    assertTrue(datastore.createCollection(COLLECTION_NAME, null));

    // We should receive non-null collection.
    assertNotNull(datastore.getCollection(COLLECTION_NAME));

    assertTrue(datastore.listCollections().contains("default_db." + COLLECTION_NAME));

    // Delete the collection.
    assertTrue(datastore.deleteCollection(COLLECTION_NAME));
  }

  @Test
  public void testUpsertWithCondition() throws Exception {
    // DocStoreTest.testUpsertWithCondition(datastore, DocStoreTest.MONGO_STORE);
  }

  @Test
  public void testUpsertWithConditionInMultipleThreads() throws Exception {
    // DocStoreTest.testWithCondition(datastore, DocStoreTest.MONGO_STORE);
  }

  private static class TestRunnable extends Thread {
    private Datastore datastore;
    private Collection collection;
    private int setValue;
    private boolean isSuccess;

    public TestRunnable(Datastore datastore, int setValue) {
      this.datastore = datastore;
      this.collection = datastore.getCollection(COLLECTION_NAME);
      this.setValue = setValue;
      this.isSuccess = false;
    }

    private int getSetValue() {
      return setValue;
    }

    private boolean getIsSuccess() {
      return isSuccess;
    }

    @Override
    public void run() {
      try {
        // fetch the document and update
        Filter condition = new Filter(Op.EQ, "size", 0);
        isSuccess =
            collection.upsert(
                new SingleValueKey("default", "testKey1"),
                Utils.createDocument(
                    ImmutablePair.of("id", "testKey1"),
                    ImmutablePair.of("name", "abc1"),
                    ImmutablePair.of("size", this.setValue),
                    ImmutablePair.of("isCostly", false)),
                condition);
      } catch (Exception e) {
        // ignore
      }
    }
  }

  public static void testWithCondition(Datastore datastore, String dataStoreName) throws Exception {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    List<TestRunnable> threads = new ArrayList<TestRunnable>();
    for (int i = 0; i < 10; i++) {
      threads.add(new TestRunnable(datastore, i + 1));
    }
    threads.stream().forEach(t -> t.start());
    threads.stream()
        .forEach(
            t -> {
              try {
                t.join();
              } catch (Exception e) {
              }
            });

    // check only one thread successfully inserted
    Map<String, List<TestRunnable>> resultMap = new HashMap<>();
    threads.stream()
        .forEach(
            t -> {
              if (t.getIsSuccess()) {
                List<TestRunnable> successList = resultMap.get("success");
                if (successList == null) {
                  successList = new ArrayList<>();
                }
                successList.add(t);
              } else {
                List<TestRunnable> failList = resultMap.get("fail");
                if (failList == null) {
                  failList = new ArrayList<>();
                }
                failList.add(t);
              }
            });
    Assertions.assertEquals(1, resultMap.get("success").size());
    Assertions.assertEquals(9, resultMap.get("fail").size());

    // assert
    // fetch document check the size
    Query query = new Query();
    query.setFilter(Filter.eq("_id", "default:testKey1"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 1);
    Map<String, Object> doc = OBJECT_MAPPER.readValue(documents.get(0).toJson(), Map.class);

    Assertions.assertEquals(resultMap.get("success").get(0).getSetValue(), (int) doc.get("size"));
  }

  public static void testUpsertWithCondition(Datastore datastore, String dataStoreName)
      throws Exception {
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    Query query = new Query();
    query.setFilter(Filter.eq("_id", "default:testKey1"));
    Filter condition = new Filter(Op.EQ, "isCostly", false);

    // test that document document is inserted if its not exists
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 0);

    boolean bool =
        collection.upsert(
            new SingleValueKey("default", "testKey1"),
            Utils.createDocument(
                ImmutablePair.of("id", "testKey1"),
                ImmutablePair.of("name", "abc1"),
                ImmutablePair.of("size", -10),
                ImmutablePair.of("isCostly", false)),
            condition);

    Assertions.assertTrue(bool);

    // test that document is updated if condition met
    bool =
        collection.upsert(
            new SingleValueKey("default", "testKey1"),
            Utils.createDocument(
                ImmutablePair.of("id", "testKey1"),
                ImmutablePair.of("name", "abc1"),
                ImmutablePair.of("size", 10),
                ImmutablePair.of("isCostly", false)),
            condition);

    Assertions.assertTrue(bool);

    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 1);
    documents.get(0).toJson().contains("\"size\":\"10\"");

    // test that document is not updated if condition not met
    condition = new Filter(Op.EQ, "isCostly", true);
    bool =
        collection.upsert(
            new SingleValueKey("default", "testKey1"),
            Utils.createDocument(
                ImmutablePair.of("id", "testKey1"),
                ImmutablePair.of("name", "abc1"),
                ImmutablePair.of("size", 20),
                ImmutablePair.of("isCostly", true)),
            condition);

    Assertions.assertFalse(bool);

    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 1);
    documents.get(0).toJson().contains("\"size\":\"10\"");
    documents.get(0).toJson().contains("\"isCostly\":\"false\"");
  }

  @Test
  public void testUpsertAndReturn() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("foo1", "bar1");
    Document document = new JSONDocument(objectNode);
    Document persistedDocument =
        collection.upsertAndReturn(new SingleValueKey("default", "testKey"), document);

    Query query = new Query();
    query.setFilter(Filter.eq("_id", "default:testKey"));
    // Assert upsert and search results match
    assertEquals(collection.search(query).next(), persistedDocument);

    JsonNode node = OBJECT_MAPPER.readTree(persistedDocument.toJson());
    String lastUpdatedTime = node.findValue(LAST_UPDATE_TIME_KEY).findValue("$date").asText();
    long createdTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();

    objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("foo2", "bar2");
    document = new JSONDocument(objectNode);

    // Upsert again and verify that createdTime does not change, while lastUpdatedTime
    // has changed and values have merged
    Document updatedDocument =
        collection.upsertAndReturn(new SingleValueKey("default", "testKey"), document);
    node = OBJECT_MAPPER.readTree(updatedDocument.toJson());
    String newLastUpdatedTime = node.findValue(LAST_UPDATE_TIME_KEY).findValue("$date").asText();
    long newCreatedTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();
    assertEquals(createdTime, newCreatedTime);
    assertNotEquals(lastUpdatedTime, newLastUpdatedTime);

    assertEquals("bar1", node.get("foo1").asText());
    assertEquals("bar2", node.get("foo2").asText());
  }

  @Test
  public void testBulkUpsertAndVerifyUpdatedTime() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("foo1", "bar1");
    Document document = new JSONDocument(objectNode);
    collection.bulkUpsert(Map.of(new SingleValueKey("default", "testKey"), document));

    Query query = new Query();
    query.setFilter(Filter.eq("_id", "default:testKey"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    String persistedDocument = documents.get(0).toJson();
    // Assert _lastUpdateTime fields exists
    Assertions.assertTrue(persistedDocument.contains(LAST_UPDATE_TIME_KEY));
    Assertions.assertTrue(persistedDocument.contains(LAST_UPDATED_TIME_KEY));
    Assertions.assertTrue(persistedDocument.contains(LAST_CREATED_TIME_KEY));

    JsonNode node = OBJECT_MAPPER.readTree(persistedDocument);
    String lastUpdateTime = node.findValue(LAST_UPDATE_TIME_KEY).findValue("$date").asText();
    long updatedTime = node.findValue(LAST_UPDATED_TIME_KEY).asLong();
    long createdTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();
    // Upsert again and verify that createdTime does not change, while lastUpdatedTime
    // has changed
    collection.bulkUpsert(Map.of(new SingleValueKey("default", "testKey"), document));
    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    persistedDocument = documents.get(0).toJson();
    node = OBJECT_MAPPER.readTree(persistedDocument);
    String newLastUpdateTime = node.findValue(LAST_UPDATE_TIME_KEY).findValue("$date").asText();
    long newUpdatedTime = node.findValue(LAST_UPDATED_TIME_KEY).asLong();
    long newCreatedTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();
    Assertions.assertEquals(createdTime, newCreatedTime);
    Assertions.assertFalse(newLastUpdateTime.equalsIgnoreCase(lastUpdateTime));
    Assertions.assertNotEquals(newUpdatedTime, updatedTime);
  }

  @Test
  public void testSelectAll() throws IOException {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey1"), Utils.createDocument("testKey1", "abc1"));
    collection.upsert(
        new SingleValueKey("default", "testKey2"), Utils.createDocument("testKey2", "abc2"));
    assertEquals(2, collection.count());
    Iterator<Document> iterator = collection.search(new Query());
    List<Document> documents = new ArrayList<>();
    while (iterator.hasNext()) {
      documents.add(iterator.next());
    }
    assertEquals(2, documents.size());

    // Delete one of the documents and test again.
    collection.delete(new SingleValueKey("default", "testKey1"));
    assertEquals(1, collection.count());
  }

  @Test
  public void testSelections() throws IOException {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey1"), Utils.createDocument("testKey1", "abc1"));
    collection.upsert(
        new SingleValueKey("default", "testKey2"), Utils.createDocument("testKey2", "abc2"));
    assertEquals(2, collection.count());
    Query query = new Query();
    query.addSelection("testKey1");
    Iterator<Document> iterator = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (iterator.hasNext()) {
      documents.add(iterator.next());
    }
    assertEquals(2, documents.size());

    Assertions.assertFalse(documents.isEmpty());

    String document1 = documents.get(0).toJson();
    Assertions.assertTrue(document1.contains("testKey1"));
    JsonNode node1 = OBJECT_MAPPER.readTree(document1);
    String value = node1.findValue("testKey1").asText();
    Assertions.assertEquals("abc1", value);

    String document2 = documents.get(1).toJson();
    Assertions.assertFalse(document2.contains("testKey1"));
    JsonNode node2 = OBJECT_MAPPER.readTree(document2);
    assertTrue(node2.isEmpty());
  }

  /**
   * This is an example where same field is having different type values. e.g size field has
   * boolean, numeric and string values. This is a valid scenario for document store, and works fine
   * with mongodb. However, there is currently limitation on postgres as document store
   * implementation using jsonb, and it won't work.
   */
  @Test
  public void testWithDifferentDataTypes() throws IOException {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    // size field with integer value
    collection.upsert(
        new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of("name", "abc1"),
            ImmutablePair.of("size", -10)));
    collection.upsert(
        new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of("name", "abc2"),
            ImmutablePair.of("size", -20)));

    // size field with string value
    collection.upsert(
        new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("name", "abc3"),
            ImmutablePair.of("size", false)));
    collection.upsert(
        new SingleValueKey("default", "testKey4"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey4"),
            ImmutablePair.of("name", "abc4"),
            ImmutablePair.of("size", true)));

    // size field with boolean value
    collection.upsert(
        new SingleValueKey("default", "testKey5"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey5"),
            ImmutablePair.of("name", "abc5"),
            ImmutablePair.of("size", "10")));
    collection.upsert(
        new SingleValueKey("default", "testKey6"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey6"),
            ImmutablePair.of("name", "abc6"),
            ImmutablePair.of("size", "20")));

    // query for size field with integer value
    Query queryGt = new Query();
    Filter filterGt = new Filter(Op.GT, "size", -30);
    queryGt.setFilter(filterGt);
    Iterator<Document> results = collection.search(queryGt);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(2, documents.size());

    // query for size field with string value
    Query queryGtStr = new Query();
    Filter filterGtStr = new Filter(Op.GT, "size", "1");
    queryGtStr.setFilter(filterGtStr);
    Iterator<Document> resultsGtStr = collection.search(queryGtStr);
    List<Document> documentsGtStr = new ArrayList<>();
    while (resultsGtStr.hasNext()) {
      documentsGtStr.add(resultsGtStr.next());
    }
    Assertions.assertEquals(2, documentsGtStr.size());

    // query for size field with boolean value
    Query queryGtBool = new Query();
    Filter filterGtBool = new Filter(Op.GT, "size", false);
    queryGtStr.setFilter(filterGtBool);
    Iterator<Document> resultsGtBool = collection.search(queryGtStr);
    List<Document> documentsGtBool = new ArrayList<>();
    while (resultsGtBool.hasNext()) {
      documentsGtBool.add(resultsGtBool.next());
    }
    Assertions.assertEquals(1, documentsGtBool.size());

    datastore.deleteCollection(COLLECTION_NAME);
  }

  @Test
  public void testReturnAndBulkUpsert() throws IOException {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Document> documentMapV1 =
        Map.of(
            new SingleValueKey("default", "testKey1"),
                Utils.createDocument("id", "1", "testKey1", "abc-v1"),
            new SingleValueKey("default", "testKey2"),
                Utils.createDocument("id", "2", "testKey2", "xyz-v1"));

    Iterator<Document> iterator = collection.bulkUpsertAndReturnOlderDocuments(documentMapV1);
    // Initially there shouldn't be any documents.
    Assertions.assertFalse(iterator.hasNext());

    // Add more details to the document and bulk upsert again.
    Map<Key, Document> documentMapV2 =
        Map.of(
            new SingleValueKey("default", "testKey1"),
                Utils.createDocument("id", "1", "testKey1", "abc-v2"),
            new SingleValueKey("default", "testKey2"),
                Utils.createDocument("id", "2", "testKey2", "xyz-v2"));
    iterator = collection.bulkUpsertAndReturnOlderDocuments(documentMapV2);
    assertEquals(2, collection.count());
    List<Document> documents = new ArrayList<>();
    while (iterator.hasNext()) {
      documents.add(iterator.next());
    }
    assertEquals(2, documents.size());

    Map<String, JsonNode> expectedDocs = convertToMap(documentMapV1.values(), "id");
    Map<String, JsonNode> actualDocs = convertToMap(documents, "id");

    // Verify that the documents returned were previous copies.
    for (Map.Entry<String, JsonNode> entry : expectedDocs.entrySet()) {
      JsonNode expected = entry.getValue();
      JsonNode actual = actualDocs.get(entry.getKey());

      Assertions.assertNotNull(actual);
      JsonNode patch = JsonDiff.asJson(expected, actual);

      // Verify that there are only additions and "no" removals in this new node.
      Set<String> ops = new HashSet<>();
      patch
          .elements()
          .forEachRemaining(
              e -> {
                if (e.has("op")) {
                  ops.add(e.get("op").asText());
                }
              });

      Assertions.assertTrue(ops.contains("add"));
      Assertions.assertEquals(1, ops.size());
    }

    // Delete one of the documents and test again.
    collection.delete(new SingleValueKey("default", "testKey1"));
    assertEquals(1, collection.count());
  }

  private Map<String, JsonNode> convertToMap(java.util.Collection<Document> docs, String key) {
    return docs.stream()
        .map(
            d -> {
              try {
                return OBJECT_MAPPER.reader().readTree(d.toJson());
              } catch (JsonProcessingException e) {
                e.printStackTrace();
              }
              return null;
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(d -> d.get(key).asText(), d -> d));
  }

  @Test
  public void testLike() {
    MongoClient mongoClient =
        MongoClients.create("mongodb://localhost:" + mongo.getMappedPort(27017).toString());

    MongoDatabase db = mongoClient.getDatabase("default_db");
    String collectionName = "myTest2";
    MongoCollection<BasicDBObject> myTest2 = db.getCollection(collectionName, BasicDBObject.class);
    myTest2.drop();

    {
      BasicDBObject basicDBObject = new BasicDBObject();
      basicDBObject.put("testKey1", "abc1");
      myTest2.insertOne(basicDBObject);
    }
    {
      BasicDBObject basicDBObject = new BasicDBObject();
      basicDBObject.put("testKey1", "xyz1");
      myTest2.insertOne(basicDBObject);
    }
    {
      BasicDBObject basicDBObject = new BasicDBObject();
      basicDBObject.put("testKey2", "abc2");
      myTest2.insertOne(basicDBObject);
    }

    FindIterable<BasicDBObject> result =
        myTest2.find(new BasicDBObject("testKey1", new BasicDBObject("$regex", "abc")));
    MongoCursor<BasicDBObject> cursor = result.cursor();
    List<DBObject> results = new ArrayList<>();
    while (cursor.hasNext()) {
      DBObject dbObject = cursor.next();
      results.add(dbObject);
      System.out.println(dbObject);
    }
    assertEquals(1, results.size());
  }

  @Test
  public void testContains() throws IOException {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    // https://docs.mongodb.com/manual/reference/operator/query/elemMatch/
    collection.upsert(
        new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of(
                "products",
                List.of(
                    Map.of("product", "abc", "score", 10), Map.of("product", "xyz", "score", 5)))));

    collection.upsert(
        new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of(
                "products",
                List.of(
                    Map.of("product", "abc", "score", 8), Map.of("product", "xyz", "score", 7)))));

    collection.upsert(
        new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of(
                "products",
                List.of(
                    Map.of("product", "abc", "score", 7), Map.of("product", "xyz", "score", 8)))));

    collection.upsert(
        new SingleValueKey("default", "testKey4"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey4"),
            ImmutablePair.of(
                "products",
                List.of(
                    Map.of("product", "abc", "score", 7), Map.of("product", "def", "score", 8)))));

    // try with contains filter
    Query query = new Query();
    Filter filter = new Filter(Op.CONTAINS, "products", Map.of("product", "xyz"));
    query.setFilter(filter);
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(3, documents.size());

    documents.forEach(
        document -> {
          String jsonStr = document.toJson();
          assertTrue(
              jsonStr.contains("\"id\":\"testKey1\"")
                  || document.toJson().contains("\"id\":\"testKey2\"")
                  || document.toJson().contains("\"id\":\"testKey3\""));
        });
  }
}
