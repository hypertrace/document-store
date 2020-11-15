package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

public class PostgresDocStoreTest {
  public static final String ID_KEY = "id";
  public static final String DOCUMENT_KEY = "document";
  private static final String COLLECTION_NAME = "mytest2";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String UPDATED_AT = "updated_at";
  private static final String CREATED_AT = "created_at";
  
  private static Datastore datastore;
  
  
  @BeforeAll
  public static void init() {
    
    DatastoreProvider.register("POSTGRES", PostgresDatastore.class);
    
    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.putIfAbsent("url", "jdbc:postgresql://localhost:5432/");
    postgresConfig.putIfAbsent("user", "postgres");
    postgresConfig.putIfAbsent("password", "postgres");
    postgresConfig.putIfAbsent("type", "jsonb");
    Config config = ConfigFactory.parseMap(postgresConfig);
    
    datastore = DatastoreProvider.getDatastore("Postgres", config);
    System.out.println(datastore.listCollections());
  }
  
  @BeforeEach
  public void cleanup() {
    datastore.deleteCollection(COLLECTION_NAME);
    datastore.createCollection(COLLECTION_NAME, null);
  }
  
  @Test
  public void testUpsert() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey"), createDocument("foo1", "bar1"));
    
    Query query = new Query();
    query.setFilter(Filter.eq(ID_KEY, "default:testKey"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    for (; results.hasNext(); ) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    String persistedDocument = documents.get(0).toJson();
    // Assert _lastUpdateTime fields exists
    Assertions.assertTrue(persistedDocument.contains(UPDATED_AT));
    Assertions.assertTrue(persistedDocument.contains(CREATED_AT));
  }
  
  @Test
  public void testBulkUpsert() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Document> bulkMap = new HashMap<>();
    bulkMap.put(new SingleValueKey("default", "testKey1"), createDocument("name", "Bob"));
    bulkMap.put(new SingleValueKey("default", "testKey2"), createDocument("name", "Alice"));
    bulkMap.put(new SingleValueKey("default", "testKey3"), createDocument("name", "Alice"));
    bulkMap.put(new SingleValueKey("default", "testKey4"), createDocument("name", "Bob"));
    bulkMap.put(new SingleValueKey("default", "testKey5"), createDocument("name", "Alice"));
    bulkMap.put(
      new SingleValueKey("default", "testKey6"), createDocument("email", "bob@example.com"));
    
    collection.bulkUpsert(bulkMap);
    
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
  public void testSubDocumentUpdate() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, createDocument("foo1", "bar1"));
    
    Document subDocument = createDocument("subfoo1", "subbar1");
    collection.updateSubDoc(docKey, "subdoc", subDocument);
    
    Document nestedDocument = createDocument("nestedfoo1", "nestedbar1");
    collection.updateSubDoc(docKey, "subdoc.nesteddoc", nestedDocument);
    
    Query query = new Query();
    query.setFilter(Filter.eq(ID_KEY, "default:testKey"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    for (; results.hasNext(); ) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    Assertions.assertTrue(documents.get(0).toJson().contains("subdoc"));
    Assertions.assertTrue(documents.get(0).toJson().contains("nesteddoc"));
  }
  
  @Test
  public void testSubDocumentDelete() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, createDocument("foo1", "bar1"));
    
    Document subDocument = createDocument("subfoo1", "subbar1");
    collection.updateSubDoc(docKey, "subdoc", subDocument);
    
    boolean status = collection.deleteSubDoc(docKey, "subdoc.subfoo1");
    Assertions.assertTrue(status);
    
    status = collection.deleteSubDoc(docKey, "subdoc");
    Assertions.assertTrue(status);
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
      for (; results.hasNext(); ) {
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
  public void testOffsetLimitAndOrderBY() throws IOException {
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
      query.addOrderBy(new OrderBy(ID_KEY, true));
      
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
  
  
  private Document createDocument(String key, String value) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put(key, value);
    return new JSONDocument(objectNode);
  }
  
}
