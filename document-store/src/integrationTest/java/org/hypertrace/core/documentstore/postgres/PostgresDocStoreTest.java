package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.utils.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PostgresDocStoreTest {

  public static final String ID = "id";
  public static final String DOCUMENT_ID = "_id";
  public static final String UPDATED_AT = "updated_at";
  public static final String CREATED_AT = "created_at";
  private static final String COLLECTION_NAME = "mytest";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static Datastore datastore;


  @BeforeAll
  public static void init() {

    DatastoreProvider.register("POSTGRES", PostgresDatastore.class);

    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.putIfAbsent("url", "jdbc:postgresql://localhost:5432/");
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
      System.out.println("Exception executing init test with user and password");
      Assertions.fail();
    }

  }

  @Test
  public void testUpsert() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    boolean result = collection
        .upsert(new SingleValueKey("default", "testKey"), Utils.createDocument("foo1", "bar1"));
    Assertions.assertTrue(result);

    Query query = new Query();
    query.setFilter(Filter.eq(ID, "default:testKey"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());
    String persistedDocument = documents.get(0).toJson();
    Assertions.assertTrue(persistedDocument.contains(UPDATED_AT));
    Assertions.assertTrue(persistedDocument.contains(CREATED_AT));
  }

  @Test
  public void testUpsertAndReturn() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Document document = Utils.createDocument("foo1", "bar1");
    Document resultDocument = collection
        .upsertAndReturn(new SingleValueKey("default", "testKey"), document);

    Assertions.assertEquals(document.toJson(), resultDocument.toJson());
  }

  @Test
  public void testBulkUpsert() {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Document> bulkMap = new HashMap<>();
    bulkMap.put(new SingleValueKey("default", "testKey1"), Utils.createDocument("name", "Bob"));
    bulkMap.put(new SingleValueKey("default", "testKey2"), Utils.createDocument("name", "Alice"));
    bulkMap.put(new SingleValueKey("default", "testKey3"), Utils.createDocument("name", "Alice"));
    bulkMap.put(new SingleValueKey("default", "testKey4"), Utils.createDocument("name", "Bob"));
    bulkMap.put(new SingleValueKey("default", "testKey5"), Utils.createDocument("name", "Alice"));
    bulkMap.put(
        new SingleValueKey("default", "testKey6"), Utils.createDocument("email", "bob@example.com"));

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
  public void testBulkUpsertAndReturn() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Document> bulkMap = new HashMap<>();
    bulkMap.put(new SingleValueKey("default", "testKey1"), Utils.createDocument("name", "Bob"));
    bulkMap.put(new SingleValueKey("default", "testKey2"), Utils.createDocument("name", "Alice"));
    bulkMap.put(new SingleValueKey("default", "testKey3"), Utils.createDocument("name", "Alice"));
    bulkMap.put(new SingleValueKey("default", "testKey4"), Utils.createDocument("name", "Bob"));
    bulkMap.put(new SingleValueKey("default", "testKey5"), Utils.createDocument("name", "Alice"));
    bulkMap.put(
        new SingleValueKey("default", "testKey6"), Utils.createDocument("email", "bob@example.com"));

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
  public void testSubDocumentUpdate() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));

    Document subDocument = Utils.createDocument("subfoo1", "subbar1");
    collection.updateSubDoc(docKey, "subdoc", subDocument);

    Document nestedDocument = Utils.createDocument("nestedfoo1", "nestedbar1");
    collection.updateSubDoc(docKey, "subdoc.nesteddoc", nestedDocument);

    Query query = new Query();
    query.setFilter(Filter.eq(ID, "default:testKey"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());

    ObjectNode jsonNode = (ObjectNode) OBJECT_MAPPER.readTree(documents.get(0).toJson());
    jsonNode.remove(CREATED_AT);
    jsonNode.remove(UPDATED_AT);
    String expected = "{\"foo1\":\"bar1\",\"subdoc\":{\"subfoo1\":\"subbar1\",\"nesteddoc\":{\"nestedfoo1\":\"nestedbar1\"}}}";
    Assertions.assertEquals(expected, OBJECT_MAPPER.writeValueAsString(jsonNode));
  }

  @Test
  public void testSubDocumentDelete() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));

    Document subDocument = Utils.createDocument("subfoo1", "subbar1");
    collection.updateSubDoc(docKey, "subdoc", subDocument);

    boolean status = collection.deleteSubDoc(docKey, "subdoc.subfoo1");
    Assertions.assertTrue(status);

    status = collection.deleteSubDoc(docKey, "subdoc");
    Assertions.assertTrue(status);
  }

  @Test
  public void testCount() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));
    Assertions.assertEquals(collection.count(), 1);
  }

  @Test
  public void testDelete() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));

    Assertions.assertEquals(collection.count(), 1);
    collection.delete(docKey);
    Assertions.assertEquals(collection.count(), 0);

  }

  @Test
  public void testDeleteAll() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));

    Assertions.assertEquals(collection.count(), 1);
    collection.deleteAll();
    Assertions.assertEquals(collection.count(), 0);
  }

  @Test
  public void testDrop() {
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    Assertions.assertTrue(datastore.listCollections().contains("postgres." + COLLECTION_NAME));
    collection.drop();
    Assertions.assertFalse(datastore.listCollections().contains("postgres." + COLLECTION_NAME));
  }

  @Test
  public void testIgnoreCaseLikeQuery() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey"), Utils.createDocument("name", "Bob"));

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
  public void testInQuery() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"), Utils.createDocument("name", "Bob"));
    collection.upsert(new SingleValueKey("default", "testKey2"), Utils.createDocument("name", "Alice"));
    collection.upsert(new SingleValueKey("default", "testKey3"), Utils.createDocument("name", "Halo"));

    List<String> inArray = new ArrayList<>();
    inArray.add("Bob");
    inArray.add("Alice");

    Query query = new Query();
    query.setFilter(new Filter(Filter.Op.IN, "name", inArray));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(documents.size(), 2);
  }

  @Test
  public void testInQueryWithNumberField() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of("name", "abc1"),
            ImmutablePair.of("size", -10.2),
            ImmutablePair.of("isCostly", false)));
    collection.upsert(new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of("name", "abc2"),
            ImmutablePair.of("size", 10.4),
            ImmutablePair.of("isCostly", false)));
    collection.upsert(new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("name", "abc3"),
            ImmutablePair.of("size", 30),
            ImmutablePair.of("isCostly", false)));

    List<Number> inArray = new ArrayList<>();
    inArray.add(10.4);
    inArray.add(30.1);

    Query query = new Query();
    query.setFilter(new Filter(Filter.Op.IN, "size", inArray));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(documents.size(), 1);
  }

  @Test
  public void testNotInQueryWithNumberField() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of("name", "abc1"),
            ImmutablePair.of("size", -10.2),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("tags", List.of("black", "white")),
            ImmutablePair.of("color", "red")));
    collection.upsert(new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of("name", "abc2"),
            ImmutablePair.of("size", 10.4),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("tags", List.of("gray")),
            ImmutablePair.of("color", "gray")));
    collection.upsert(new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("name", "abc3"),
            ImmutablePair.of("size", 30),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("tags", List.of("brown")),
            ImmutablePair.of("color", "blue")));
    collection.upsert(new SingleValueKey("default", "testKey4"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey4"),
            ImmutablePair.of("name", "abc4"),
            ImmutablePair.of("size", 10.4),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("tags", List.of("gray")),
            ImmutablePair.of("color", "pink")));

    collection.upsert(new SingleValueKey("default", "testKey5"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey5"),
            ImmutablePair.of("name", "abc5")));

    collection.updateSubDoc(new SingleValueKey("default", "testKey1"),
        "subdoc", Utils.createDocument("nestedkey1", "pqr1"));
    collection.updateSubDoc(new SingleValueKey("default", "testKey2"),
        "subdoc", Utils.createDocument("nestedkey1", "pqr2"));
    collection.updateSubDoc(new SingleValueKey("default", "testKey3"),
        "subdoc", Utils.createDocument("nestedkey1", "pqr3"));


    // check with string filed
    List<String> names = new ArrayList<>();
    names.add("abc3");
    names.add("abc2");

    Query query = new Query();
    query.setFilter(new Filter(Filter.Op.NOT_IN, "name", names));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(3, documents.size());
    documents.forEach(document -> {
      String jsonStr = document.toJson();
      assertTrue(jsonStr.contains("\"name\":\"abc1\"")
          || jsonStr.contains("\"name\":\"abc4\"")
          || jsonStr.contains("\"name\":\"abc5\""));
    });

    // check with multiple operator and + not_in with string field
    List<String> colors = new ArrayList<>();
    colors.add("red");
    colors.add("pink");

    query = new Query();
    Filter[] filters = new Filter[2];
    filters[0] = new Filter(Op.EQ, "size", 10.4);
    filters[1] = new Filter(Filter.Op.NOT_IN, "color", colors);
    Filter f = new Filter();
    f.setOp(Op.OR);
    f.setChildFilters(filters);
    query.setFilter(f);
    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(4, documents.size());
    documents.forEach(document -> {
      String jsonStr = document.toJson();
      assertTrue(jsonStr.contains("\"name\":\"abc2\"")
          || jsonStr.contains("\"name\":\"abc3\"")
          || jsonStr.contains("\"name\":\"abc4\"")
          || jsonStr.contains("\"name\":\"abc5\""));
    });

    // check with numeric field
    List<Number> sizes = new ArrayList<>();
    sizes.add(-10.2);
    sizes.add(10.4);

    query = new Query();
    query.setFilter(new Filter(Filter.Op.NOT_IN, "size", sizes));
    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(2, documents.size());
    documents.forEach(document -> {
      String jsonStr = document.toJson();
      assertTrue(jsonStr.contains("\"name\":\"abc3\"")
          || jsonStr.contains("\"name\":\"abc5\""));
    });

    // check with multiple operator and + not_in with numeric field
    sizes = new ArrayList<>();
    sizes.add(-10.2);
    sizes.add(10.4);

    query = new Query();
    filters = new Filter[2];
    filters[0] = new Filter(Op.EQ, "color", "pink");
    filters[1] = new Filter(Filter.Op.NOT_IN, "size", sizes);
    f = new Filter();
    f.setOp(Op.OR);
    f.setChildFilters(filters);
    query.setFilter(f);
    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(3, documents.size());
    documents.forEach(document -> {
      String jsonStr = document.toJson();
      assertTrue(jsonStr.contains("\"name\":\"abc3\"")
          || jsonStr.contains("\"name\":\"abc4\"")
          || jsonStr.contains("\"name\":\"abc5\""));
    });

    // check for subDoc key
    List<String> subDocs = new ArrayList<>();
    subDocs.add("pqr1");
    subDocs.add("pqr2");

    query = new Query();
    query.setFilter(new Filter(Op.NOT_IN, "subdoc.nestedkey1", subDocs));
    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    assertEquals(3, documents.size());
    documents.forEach(document -> {
      String jsonStr = document.toJson();
      assertTrue(jsonStr.contains("\"name\":\"abc3\"")
          || jsonStr.contains("\"name\":\"abc4\"")
          || jsonStr.contains("\"name\":\"abc5\""));
    });
  }

  @Test
  public void testSearch() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    String docStr1 = "{\"amount\":1234.5,\"testKeyExist\":null,\"attributes\":{\"trace_id\":{\"value\":{\"string\":\"00000000000000005e194fdf9fbf5101\"}},\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"createdTime\":1605692185945,\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\",\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document1 = new JSONDocument(docStr1);
    SingleValueKey key1 = new SingleValueKey("default", "testKey1");
    collection.upsert(key1, document1);

    String docStr2 = "{\"amount\":1234,\"testKeyExist\":123,\"attributes\":{\"trace_id\":{\"value\":{\"testKeyExistNested\":123,\"string\":\"00000000000000005e194fdf9fbf5101\"}},\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"createdTime\":1605692185945,\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\",\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document2 = new JSONDocument(docStr2);
    SingleValueKey key2 = new SingleValueKey("default", "testKey2");
    collection.upsert(key2, document2);

    String docStr3 = "{\"attributes\":{\"trace_id\":{\"value\":{\"testKeyExistNested\":null,\"string\":\"00000000000000005e194fdf9fbf5101\"}},\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"createdTime\":1605692185945,\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\",\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document3 = new JSONDocument(docStr3);
    SingleValueKey key3 = new SingleValueKey("default", "testKey3");
    collection.upsert(key3, document3);

    // Search integer field
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, "amount", 1234));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      for (; results.hasNext(); ) {
        documents.add(results.next());
      }
      Assertions.assertEquals(1, documents.size());
    }

    // Search float field
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, "amount", 1234.5));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      for (; results.hasNext(); ) {
        documents.add(results.next());
      }
      Assertions.assertEquals(1, documents.size());
    }

    // Search integer and float field
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.GTE, "amount", 123));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      for (; results.hasNext(); ) {
        documents.add(results.next());
      }
      Assertions.assertEquals(2, documents.size());
    }

    // Search _id field in the document
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, DOCUMENT_ID, key1.toString()));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      for (; results.hasNext(); ) {
        documents.add(results.next());
      }
      Assertions.assertEquals(1, documents.size());
    }

    // Unsupported Object Type in Filter, should throw an UnsupportedOperationException
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, "amount", new Filter()));
      String expected = "Un-supported object types in filter";
      Exception exception = assertThrows(UnsupportedOperationException.class,
              () -> collection.search(query));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expected));
    }

    // Field exists in the document
    {
      Query query = new Query();
      query.setFilter(new Filter(Op.EXISTS, "testKeyExist", null));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      Assertions.assertEquals(2, documents.size());
    }

    // Nested Field exists in the document
    {
      Query query = new Query();
      query.setFilter(new Filter(Op.EXISTS, "attributes.trace_id.value.testKeyExistNested", null));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      Assertions.assertEquals(2, documents.size());
    }

    // Field Not Exists in the document
    {
      Query query = new Query();
      query.setFilter(new Filter(Op.NOT_EXISTS, "attributes.trace_id.value.testKeyExistNested", null));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      Assertions.assertEquals(1, documents.size());
    }

  }

  @Test
  public void testExistsFilter() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"),
            Utils.createDocument(
                    ImmutablePair.of("id", "testKey1"),
                    ImmutablePair.of("name", "abc1"),
                    ImmutablePair.of("size", -10.2),
                    ImmutablePair.of("isCostly", false)));
    collection.upsert(new SingleValueKey("default", "testKey2"),
            Utils.createDocument(
                    ImmutablePair.of("id", "testKey2"),
                    ImmutablePair.of("name", "abc2"),
                    ImmutablePair.of("size", 10.4),
                    ImmutablePair.of("isCostly", false)));
    collection.upsert(new SingleValueKey("default", "testKey3"),
            Utils.createDocument(
                    ImmutablePair.of("id", "testKey3"),
                    ImmutablePair.of("name", "abc3"),
                    ImmutablePair.of("size", 30),
                    ImmutablePair.of("isCostly", false),
                    ImmutablePair.of("city", "bangalore")));
    collection.upsert(new SingleValueKey("default", "testKey4"),
            Utils.createDocument(
                    ImmutablePair.of("id", "testKey4"),
                    ImmutablePair.of("name", "abc4"),
                    ImmutablePair.of("size", 30),
                    ImmutablePair.of("isCostly", false),
                    ImmutablePair.of("city", null)));
    Query query = new Query();
    query.setFilter(new Filter(Op.EXISTS, "city", true));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(documents.size(), 2);
  }

  @Test
  public void testNotExistsFilter() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"),
            Utils.createDocument(
                    ImmutablePair.of("id", "testKey1"),
                    ImmutablePair.of("name", "abc1"),
                    ImmutablePair.of("size", -10.2),
                    ImmutablePair.of("isCostly", false)));
    collection.upsert(new SingleValueKey("default", "testKey2"),
            Utils.createDocument(
                    ImmutablePair.of("id", "testKey2"),
                    ImmutablePair.of("name", "abc2"),
                    ImmutablePair.of("size", 10.4),
                    ImmutablePair.of("isCostly", false)));
    collection.upsert(new SingleValueKey("default", "testKey3"),
            Utils.createDocument(
                    ImmutablePair.of("id", "testKey3"),
                    ImmutablePair.of("name", "abc3"),
                    ImmutablePair.of("size", 30),
                    ImmutablePair.of("isCostly", false),
                    ImmutablePair.of("city", "bangalore")));
    collection.upsert(new SingleValueKey("default", "testKey4"),
            Utils.createDocument(
                    ImmutablePair.of("id", "testKey4"),
                    ImmutablePair.of("name", "abc4"),
                    ImmutablePair.of("size", 30),
                    ImmutablePair.of("isCostly", false),
                    ImmutablePair.of("city", null)));
    Query query = new Query();
    query.setFilter(new Filter(Op.EXISTS, "city", false));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(documents.size(), 2);
  }

  @Test
  public void testSearchForNestedKey() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    String documentString = "{\"attributes\":{\"trace_id\":{\"value\":{\"string\":\"00000000000000005e194fdf9fbf5101\"}},\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"createdTime\":1605692185945,\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\",\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document = new JSONDocument(documentString);
    SingleValueKey key = new SingleValueKey("default", "testKey1");
    collection.upsert(key, document);

    // Search nested field in the document
    Query query = new Query();
    query
        .setFilter(new Filter(Filter.Op.EQ, "attributes.span_id.value.string", "6449f1f720c93a67"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(documents.size(), 1);
  }

  @Test
  public void testTotalWithQuery() throws IOException {
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(new SingleValueKey("default", "testKey1"), Utils.createDocument("name", "Bob"));
    collection.upsert(new SingleValueKey("default", "testKey2"), Utils.createDocument("name", "Alice"));
    collection.upsert(new SingleValueKey("default", "testKey3"), Utils.createDocument("name", "Alice"));
    collection.upsert(new SingleValueKey("default", "testKey4"), Utils.createDocument("name", "Bob"));
    collection.upsert(new SingleValueKey("default", "testKey5"), Utils.createDocument("name", "Alice"));
    collection.upsert(
        new SingleValueKey("default", "testKey6"), Utils.createDocument("email", "bob@example.com"));

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
    collection.upsert(new SingleValueKey("default", "testKey1"), Utils.createDocument("foo1", "bar1"));
    collection.upsert(new SingleValueKey("default", "testKey2"), Utils.createDocument("foo2", "bar2"));
    collection.upsert(new SingleValueKey("default", "testKey3"), Utils.createDocument("foo3", "bar3"));
    collection.upsert(new SingleValueKey("default", "testKey4"), Utils.createDocument("foo4", "bar4"));
    collection.upsert(new SingleValueKey("default", "testKey5"), Utils.createDocument("foo5", "bar5"));

    // Querying 5 times, to make sure the order of results is maintained with offset + limit
    for (int i = 0; i < 5; i++) {
      Query query = new Query();
      query.setLimit(2);
      query.setOffset(1);
      query.addOrderBy(new OrderBy(DOCUMENT_ID, true));

      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
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
  public void testWithDifferentFieldTypes() throws IOException {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    // size field with integer value, isCostly boolean field
    collection.upsert(new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of("name", "abc1"),
            ImmutablePair.of("size", -10),
            ImmutablePair.of("isCostly", false))
    );

    collection.upsert(new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of("name", "abc2"),
            ImmutablePair.of("size", -20),
            ImmutablePair.of("isCostly", false))
    );

    collection.upsert(new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("name", "abc3"),
            ImmutablePair.of("size", 5),
            ImmutablePair.of("isCostly", true))
    );

    collection.upsert(new SingleValueKey("default", "testKey4"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey4"),
            ImmutablePair.of("name", "abc4"),
            ImmutablePair.of("size", 10),
            ImmutablePair.of("isCostly", true))
    );

    // query field having int type
    Query queryNumericField = new Query();
    Filter filter = new Filter(Op.GT, "size", -30);
    queryNumericField.setFilter(filter);
    Iterator<Document> results = collection.search(queryNumericField);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(4, documents.size());

    // query field having boolean field
    Query queryBooleanField = new Query();
    filter = new Filter(Op.GT, "isCostly", false);
    queryBooleanField.setFilter(filter);
    results = collection.search(queryBooleanField);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(2, documents.size());

    // query string field
    Query queryStringField = new Query();
    filter = new Filter(Op.GT, "name", "abc2");
    queryStringField.setFilter(filter);
    results = collection.search(queryBooleanField);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(2, documents.size());

    datastore.deleteCollection(COLLECTION_NAME);
  }

  @Test
  public void testNotEquals() throws IOException {
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    collection.upsert(new SingleValueKey("default", "testKey1"),
            Utils.createDocument(
                    ImmutablePair.of("key1", "abc1"),
                    ImmutablePair.of("key2", "xyz1")));
    collection.upsert(new SingleValueKey("default", "testKey2"),
            Utils.createDocument(
                    ImmutablePair.of("key1", "abc2"),
                    ImmutablePair.of("key2", "xyz2")));
    collection.upsert(new SingleValueKey("default", "testKey3"),
            Utils.createDocument(
                    ImmutablePair.of("key1", "abc3"),
                    ImmutablePair.of("key2", "xyz3")));
    collection.upsert(new SingleValueKey("default", "testKey4"),
            Utils.createDocument(
                    ImmutablePair.of("key1", "abc4")));

    collection.updateSubDoc(new SingleValueKey("default", "testKey1"),
            "subdoc", Utils.createDocument("nestedkey1", "pqr1"));
    collection.updateSubDoc(new SingleValueKey("default", "testKey2"),
            "subdoc", Utils.createDocument("nestedkey1", "pqr2"));
    collection.updateSubDoc(new SingleValueKey("default", "testKey3"),
            "subdoc", Utils.createDocument("nestedkey1", "pqr3"));

    // NEQ on ID
    {
      Query query = new Query();
      query.setFilter(new Filter(Op.NEQ, "_id", "default:testKey3"));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }

      assertEquals(3, documents.size());
      documents.forEach(document -> {
        String jsonStr = document.toJson();
        assertTrue(jsonStr.contains("\"key1\":\"abc1\"")
                || document.toJson().contains("\"key1\":\"abc2\"")
                || document.toJson().contains("\"key1\":\"abc4\""));
      });
    }


    // NEQ on document fields
    {
      Query query = new Query();
      query.setFilter(new Filter(Op.NEQ, "key1", "abc3"));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      assertEquals(3, documents.size());
      documents.forEach(document -> {
        String jsonStr = document.toJson();
        assertTrue(jsonStr.contains("\"key1\":\"abc1\"")
                || document.toJson().contains("\"key1\":\"abc2\"")
                || document.toJson().contains("\"key1\":\"abc4\""));
      });
    }

    // NEQ on non existing fields
    {
      Query query = new Query();
      query.setFilter(new Filter(Op.NEQ, "key2", "xyz2"));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      assertEquals(3, documents.size());
      documents.forEach(document -> {
        String jsonStr = document.toJson();
        assertTrue(jsonStr.contains("\"key1\":\"abc1\"")
                || document.toJson().contains("\"key1\":\"abc3\"")
                || document.toJson().contains("\"key1\":\"abc4\""));
      });
    }

    // NEQ on nested fields
    {
      Query query = new Query();
      query.setFilter(new Filter(Op.NEQ, "subdoc.nestedkey1", "pqr2"));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      assertEquals(3, documents.size());
      documents.forEach(document -> {
        String jsonStr = document.toJson();
        assertTrue(jsonStr.contains("\"key1\":\"abc1\"")
                || document.toJson().contains("\"key1\":\"abc3\"")
                || document.toJson().contains("\"key1\":\"abc4\""));
      });
    }
  }

}
