package org.hypertrace.core.documentstore.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration tests for the MongoDB doc store */
public class MongoDocStoreTest {
  private static final String COLLECTION_NAME = "myTest";
  private static Datastore datastore;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  /*
   * These 2 fields should be automatically created when upserting a doc.
   * There are downstream services that depends on this. The test should verify that
   * the string is not changed.
   */
  private static final String LAST_UPDATED_TIME_KEY = "_lastUpdateTime";
  private static final String LAST_CREATED_TIME_KEY = "createdTime";

  @BeforeAll
  public static void init() {
    DatastoreProvider.register("MONGO", MongoDatastore.class);

    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.putIfAbsent("host", "localhost");
    mongoConfig.putIfAbsent("port", "27017");
    Config config = ConfigFactory.parseMap(mongoConfig);

    datastore = DatastoreProvider.getDatastore("Mongo", config);
    System.out.println(datastore.listCollections());
  }

  @AfterEach
  public void cleanup() {
    datastore.deleteCollection(COLLECTION_NAME);
    datastore.createCollection(COLLECTION_NAME, null);
  }

  @Test
  public void testCollections() {
    for (String collection: datastore.listCollections()) {
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
  public void testIgnoreCaseLikeQuery() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey"), createDocument("name", "Bob"));

    String[] ignoreCaseSearchValues = {"Bob", "bob", "BOB", "bOB", "BO", "bO", "Ob", "OB"};

    for (String searchValue : ignoreCaseSearchValues) {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.LIKE, "name", searchValue));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      Assertions.assertFalse(documents.isEmpty());
      String persistedDocument = documents.get(0).toJson();
      Assertions.assertTrue(persistedDocument.contains("Bob"));
    }
  }

  @Test
  public void testTotalWithQuery() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"), createDocument("name", "Bob"));
    collection.upsert(new SingleValueKey("default", "testKey2"), createDocument("name", "Alice"));
    collection.upsert(new SingleValueKey("default", "testKey3"), createDocument("name", "Alice"));
    collection.upsert(new SingleValueKey("default", "testKey4"), createDocument("name", "Bob"));
    collection.upsert(new SingleValueKey("default", "testKey5"), createDocument("name", "Alice"));
    collection.upsert(
        new SingleValueKey("default", "testKey6"), createDocument("email", "bob@example.com"));

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
  public void testOffsetAndLimit() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"), createDocument("foo1", "bar1"));
    collection.upsert(new SingleValueKey("default", "testKey2"), createDocument("foo2", "bar2"));
    collection.upsert(new SingleValueKey("default", "testKey3"), createDocument("foo3", "bar3"));
    collection.upsert(new SingleValueKey("default", "testKey4"), createDocument("foo4", "bar4"));
    collection.upsert(new SingleValueKey("default", "testKey5"), createDocument("foo5", "bar5"));

    // Querying 5 times, to make sure the order of results is maintained with offset + limit
    for (int i = 0; i < 5; i++) {
      Query query = new Query();
      query.setLimit(2);
      query.setOffset(1);

      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }

      assertEquals(2, documents.size());
      String persistedDocument1 = documents.get(0).toJson();
      Assertions.assertTrue(persistedDocument1.contains("foo2"));
      String persistedDocument2 = documents.get(1).toJson();
      Assertions.assertTrue(persistedDocument2.contains("foo3"));
    }
  }

  @Test
  public void testUpsert() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("foo1", "bar1");
    Document document = new JSONDocument(objectNode);
    collection.upsert(new SingleValueKey("default", "testKey"), document);

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
    Assertions.assertTrue(persistedDocument.contains(LAST_UPDATED_TIME_KEY));
    Assertions.assertTrue(persistedDocument.contains(LAST_CREATED_TIME_KEY));
    JsonNode node = OBJECT_MAPPER.readTree(persistedDocument);
    String lastUpdatedTime = node.findValue(LAST_UPDATED_TIME_KEY).findValue("$date").asText();
    long createdTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();

    // Upsert again and verify that createdTime does not change, while lastUpdatedTime
    // has changed
    collection.upsert(new SingleValueKey("default", "testKey"), document);
    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    persistedDocument = documents.get(0).toJson();
    node = OBJECT_MAPPER.readTree(persistedDocument);
    String newLastUpdatedTime = node.findValue(LAST_UPDATED_TIME_KEY).findValue("$date").asText();
    long newCreatedTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();
    assertEquals(createdTime, newCreatedTime);
    Assertions.assertFalse(newLastUpdatedTime.equalsIgnoreCase(lastUpdatedTime));
  }


  @Test
  public void testUpsertAndReturn() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("foo1", "bar1");
    Document document = new JSONDocument(objectNode);
    Document persistedDocument = collection.upsertAndReturn(new SingleValueKey("default", "testKey"), document);

    Query query = new Query();
    query.setFilter(Filter.eq("_id", "default:testKey"));
    // Assert upsert and search results match
    assertEquals(collection.search(query).next(), persistedDocument);

    JsonNode node = OBJECT_MAPPER.readTree(persistedDocument.toJson());
    String lastUpdatedTime = node.findValue(LAST_UPDATED_TIME_KEY).findValue("$date").asText();
    long createdTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();

    objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put("foo2", "bar2");
    document = new JSONDocument(objectNode);

    // Upsert again and verify that createdTime does not change, while lastUpdatedTime
    // has changed and values have merged
    Document updatedDocument = collection.upsertAndReturn(new SingleValueKey("default", "testKey"), document);
    node = OBJECT_MAPPER.readTree(updatedDocument.toJson());
    String newLastUpdatedTime = node.findValue(LAST_UPDATED_TIME_KEY).findValue("$date").asText();
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
    Assertions.assertTrue(persistedDocument.contains(LAST_UPDATED_TIME_KEY));
    Assertions.assertTrue(persistedDocument.contains(LAST_CREATED_TIME_KEY));
    JsonNode node = OBJECT_MAPPER.readTree(persistedDocument);
    String lastUpdatedTime = node.findValue(LAST_UPDATED_TIME_KEY).findValue("$date").asText();
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
    String newLastUpdatedTime = node.findValue(LAST_UPDATED_TIME_KEY).findValue("$date").asText();
    long newCreatedTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();
    Assertions.assertEquals(createdTime, newCreatedTime);
    Assertions.assertFalse(newLastUpdatedTime.equalsIgnoreCase(lastUpdatedTime));
  }

  @Test
  public void testSubDocumentUpdate() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    ObjectNode objectNode = new ObjectMapper().createObjectNode();
    objectNode.put("foo1", "bar1");
    Document document = new JSONDocument(objectNode);
    collection.upsert(new SingleValueKey("default", "testKey"), document);

    ObjectNode subObjectNode = new ObjectMapper().createObjectNode();
    subObjectNode.put("subfoo1", "subbar1");
    Document subDocument = new JSONDocument(subObjectNode);
    collection.updateSubDoc(new SingleValueKey("default", "testKey"), "subdoc", subDocument);

    Query query = new Query();
    query.setFilter(Filter.eq("_id", "default:testKey"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    Assertions.assertTrue(documents.get(0).toJson().contains("subdoc"));
  }

  @Test
  public void testSubDocumentDelete() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    ObjectNode objectNode = new ObjectMapper().createObjectNode();
    objectNode.put("foo1", "bar1");
    Document document = new JSONDocument(objectNode);
    collection.upsert(new SingleValueKey("default", "testKey"), document);

    ObjectNode subObjectNode = new ObjectMapper().createObjectNode();
    subObjectNode.put("subfoo1", "subbar1");
    Document subDocument = new JSONDocument(subObjectNode);
    collection.updateSubDoc(docKey, "subdoc", subDocument);

    boolean status = collection.deleteSubDoc(docKey, "subdoc.subfoo1");
    Assertions.assertTrue(status);

    status = collection.deleteSubDoc(docKey, "subdoc");
    Assertions.assertTrue(status);
  }

  @Test
  public void testSelectAll() throws IOException {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"), createDocument("testKey1", "abc1"));
    collection.upsert(new SingleValueKey("default", "testKey2"), createDocument("testKey2", "abc2"));
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
  public void testBulkUpsert() {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Document> documentMap = Map.of(
        new SingleValueKey("default", "testKey1"), createDocument("testKey1", "abc1"),
        new SingleValueKey("default", "testKey2"), createDocument("testKey2", "abc2")
    );

    assertTrue(collection.bulkUpsert(documentMap));
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
  public void testLike() {
    MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

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

  private Document createDocument(String key, String value) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put(key, value);
    return new JSONDocument(objectNode);
  }
}
