package org.hypertrace.core.documentstore.mongo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
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
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the MongoDB doc store
 */
public class MongoDocStoreTest {
  private static final String COLLECTION_NAME = "mytest";
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
  public void testCountWithQuery() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"), createDocument("name", "Bob"));
    collection.upsert(new SingleValueKey("default", "testKey2"), createDocument("name", "Alice"));
    collection.upsert(new SingleValueKey("default", "testKey3"), createDocument("name", "Alice"));
    collection.upsert(new SingleValueKey("default", "testKey4"), createDocument("name", "Bob"));
    collection.upsert(new SingleValueKey("default", "testKey5"), createDocument("name", "Alice"));
    collection.upsert(new SingleValueKey("default", "testKey6"), createDocument("email", "bob@example.com"));

    {
      // empty query returns all the documents
      Query query = new Query();
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      for (; results.hasNext(); ) {
        documents.add(results.next());
      }
      Assertions.assertEquals(6, documents.size());
    }

    {
      Query query = new Query();
      query.setFilter(Filter.eq("name", "Bob"));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      for (; results.hasNext(); ) {
        documents.add(results.next());
      }
      Assertions.assertEquals(2, documents.size());
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
      for (; results.hasNext(); ) {
        documents.add(results.next());
      }

      Assertions.assertEquals(2, documents.size());
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
    for (; results.hasNext(); ) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    String persistedDocument = documents.get(0).toJson();
    //Assert _lastUpdateTime fields exists
    Assertions.assertTrue(persistedDocument.contains(LAST_UPDATED_TIME_KEY));
    Assertions.assertTrue(persistedDocument.contains(LAST_CREATED_TIME_KEY));
    JsonNode node = OBJECT_MAPPER.readTree(persistedDocument);
    String lastUpdatedtime = node.findValue(LAST_UPDATED_TIME_KEY).findValue("$date").asText();
    long createdTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();

    // Upsert again and verify that createdTime does not change, while lastUpdatedTime
    // has changed
    collection.upsert(new SingleValueKey("default", "testKey"), document);
    results = collection.search(query);
    documents = new ArrayList<>();
    for (; results.hasNext(); ) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    persistedDocument = documents.get(0).toJson();
    node = OBJECT_MAPPER.readTree(persistedDocument);
    String newLastUpdatedtime = node.findValue(LAST_UPDATED_TIME_KEY).findValue("$date").asText();
    long newCreatedTime = node.findValue(LAST_CREATED_TIME_KEY).asLong();
    Assertions.assertEquals(createdTime, newCreatedTime);
    Assertions.assertFalse(newLastUpdatedtime.equalsIgnoreCase(lastUpdatedtime));
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
    for (; results.hasNext(); ) {
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

    boolean status =
        collection.deleteSubDoc(docKey, "subdoc.subfoo1");
    Assertions.assertTrue(status);

    status =
        collection.deleteSubDoc(docKey, "subdoc");
    Assertions.assertTrue(status);
  }

  @Test
  public void testSelectAll() throws Exception {
    MongoClient mongoClient = new MongoClient("localhost", 27017);

    DB db = mongoClient.getDB("default_db");
    String collectionName = "mytest2";
    DBCollection mytest2 = db.getCollection(collectionName);
    mytest2.drop();

    {
      BasicDBObject basicDBObject = new BasicDBObject();
      basicDBObject.put("testKey1", "abc1");
      mytest2.insert(basicDBObject);
    }

    {
      BasicDBObject basicDBObject = new BasicDBObject();
      basicDBObject.put("testKey2", "abc2");
      mytest2.insert(basicDBObject);
    }

    DBCursor result = mytest2.find();
    List<DBObject> results = new ArrayList<>();
    while (result.hasNext()) {
      DBObject dbObject = result.next();
      results.add(dbObject);
      System.out.println(dbObject);
    }
    Assertions.assertEquals(2, results.size());
  }

  @Test
  public void testLike() throws Exception {
    MongoClient mongoClient = new MongoClient("localhost", 27017);

    DB db = mongoClient.getDB("default_db");
    String collectionName = "mytest2";
    DBCollection mytest2 = db.getCollection(collectionName);
    mytest2.drop();

    {
      BasicDBObject basicDBObject = new BasicDBObject();
      basicDBObject.put("testKey1", "abc1");
      mytest2.insert(basicDBObject);
    }
    {
      BasicDBObject basicDBObject = new BasicDBObject();
      basicDBObject.put("testKey1", "xyz1");
      mytest2.insert(basicDBObject);
    }
    {
      BasicDBObject basicDBObject = new BasicDBObject();
      basicDBObject.put("testKey2", "abc2");
      mytest2.insert(basicDBObject);
    }

    DBCursor result = mytest2.find(new BasicDBObject("testKey1", new BasicDBObject("$regex", "abc")));
    List<DBObject> results = new ArrayList<>();
    while (result.hasNext()) {
      DBObject dbObject = result.next();
      results.add(dbObject);
      System.out.println(dbObject);
    }
    Assertions.assertEquals(1, results.size());
  }

  private Document createDocument(String key, String value) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put(key, value);
    return new JSONDocument(objectNode);
  }
}
