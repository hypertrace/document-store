package org.hypertrace.core.documentstore;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.utils.CreateUpdateTestThread.FAILURE;
import static org.hypertrace.core.documentstore.utils.CreateUpdateTestThread.SUCCESS;
import static org.hypertrace.core.documentstore.utils.Utils.convertDocumentToMap;
import static org.hypertrace.core.documentstore.utils.Utils.convertJsonToMap;
import static org.hypertrace.core.documentstore.utils.Utils.createDocumentsFromResource;
import static org.hypertrace.core.documentstore.utils.Utils.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.MongoDatastore;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.utils.CreateUpdateTestThread;
import org.hypertrace.core.documentstore.utils.CreateUpdateTestThread.Operation;
import org.hypertrace.core.documentstore.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.google.common.collect.Maps;
import org.testcontainers.utility.DockerImageName;

public class DocStoreTest {

  public static final String MONGO_STORE = "Mongo";
  public static final String POSTGRES_STORE = "Postgres";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String COLLECTION_NAME = "myTest";

  /*
   * These 3 fields should be automatically created when upserting a doc.
   * There are downstream services that depends on this. The test should verify that
   * the string is not changed.
   */
  private static final String MONGO_LAST_UPDATE_TIME_KEY = "_lastUpdateTime";
  private static final String MONGO_LAST_UPDATED_TIME_KEY = "lastUpdatedTime";
  private static final String MONGO_CREATED_TIME_KEY = "createdTime";
  /** Postgres related time fields */
  public static final String POSTGRES_UPDATED_AT = "updated_at";

  public static final String POSTGRES_CREATED_AT = "created_at";

  private static Map<String, Datastore> datastoreMap;

  private static GenericContainer<?> mongo;
  private static GenericContainer<?> postgres;

  @BeforeAll
  public static void init() {
    datastoreMap = Maps.newHashMap();
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

    Datastore mongoDatastore = DatastoreProvider.getDatastore("Mongo", config);
    System.out.println(mongoDatastore.listCollections());

    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgres.start();

    String postgresConnectionUrl =
        String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));
    DatastoreProvider.register("POSTGRES", PostgresDatastore.class);

    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.putIfAbsent("url", postgresConnectionUrl);
    postgresConfig.putIfAbsent("user", "postgres");
    postgresConfig.putIfAbsent("password", "postgres");
    Datastore postgresDatastore =
        DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(postgresConfig));
    System.out.println(postgresDatastore.listCollections());

    datastoreMap.put(MONGO_STORE, mongoDatastore);
    datastoreMap.put(POSTGRES_STORE, postgresDatastore);
  }

  @AfterEach
  public void cleanup() {
    datastoreMap.forEach(
        (k, v) -> {
          v.deleteCollection(COLLECTION_NAME);
          v.createCollection(COLLECTION_NAME, null);
        });
  }

  @AfterAll
  public static void shutdown() {
    mongo.stop();
    postgres.stop();
  }

  @MethodSource
  private static Stream<Arguments> databaseContextProvider() {
    return Stream.of(Arguments.of(MONGO_STORE), Arguments.of(POSTGRES_STORE));
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testUpsert(String dataStoreName) throws Exception {
    Datastore datastore = datastoreMap.get(dataStoreName);
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
    verifyTimeRelatedFieldsPresent(persistedDocument, dataStoreName);
    Object createdTime = getCreatedTime(persistedDocument, dataStoreName);
    Object lastUpdatedTime = getLastUpdatedTime(persistedDocument, dataStoreName);

    // Upsert again and verify that created time does not change, while updated time
    // has changed
    collection.upsert(new SingleValueKey("default", "testKey"), document);
    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }

    Assertions.assertFalse(documents.isEmpty());
    persistedDocument = documents.get(0).toJson();
    verifyTimeRelatedFieldsPresent(persistedDocument, dataStoreName);
    Object newCreatedTime = getCreatedTime(persistedDocument, dataStoreName);
    assertEquals(createdTime, newCreatedTime);
    Object newLastUpdatedTime = getLastUpdatedTime(persistedDocument, dataStoreName);
    if (isMongo(dataStoreName)) {
      // todo: for postgres lastUpdated time is same as previous
      Assertions.assertNotEquals(lastUpdatedTime, newLastUpdatedTime);
    }
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testBulkUpsert(String dataStoreName) {
    Datastore datastore = datastoreMap.get(dataStoreName);
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

    assertTrue(collection.bulkUpsert(bulkMap));

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

    collection.delete(new SingleValueKey("default", "testKey1"));
    assertEquals(5, collection.count());
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testDeleteByDocFilter(String dataStoreName) {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Document> bulkMap = new HashMap<>();
    bulkMap.put(new SingleValueKey("default", "testKey1"), Utils.createDocument("field", "value"));
    bulkMap.put(new SingleValueKey("default", "testKey2"), Utils.createDocument("field", "value"));
    bulkMap.put(new SingleValueKey("default", "testKey3"), Utils.createDocument("field", "value"));
    bulkMap.put(new SingleValueKey("default", "testKey4"), Utils.createDocument("field", "value"));
    bulkMap.put(new SingleValueKey("default", "testKey5"), Utils.createDocument("field", "value"));
    bulkMap.put(
        new SingleValueKey("default", "testKey6"),
        Utils.createDocument("email", "bob@example.com"));

    assertTrue(collection.bulkUpsert(bulkMap));

    collection.delete(org.hypertrace.core.documentstore.Filter.eq("field", "value"));
    assertEquals(1, collection.count());
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testDeleteByFilterUnsupportedOperationException(String dataStoreName) {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Document> bulkMap = new HashMap<>();
    bulkMap.put(new SingleValueKey("default", "testKey1"), Utils.createDocument("field", "value"));
    bulkMap.put(new SingleValueKey("default", "testKey2"), Utils.createDocument("field", "value"));
    bulkMap.put(new SingleValueKey("default", "testKey3"), Utils.createDocument("field", "value"));
    bulkMap.put(new SingleValueKey("default", "testKey4"), Utils.createDocument("field", "value"));
    bulkMap.put(new SingleValueKey("default", "testKey5"), Utils.createDocument("field", "value"));
    bulkMap.put(
        new SingleValueKey("default", "testKey6"),
        Utils.createDocument("email", "bob@example.com"));

    assertTrue(collection.bulkUpsert(bulkMap));

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> collection.delete((Filter) null));
    assertTrue(exception.getMessage().contains("Filter must be provided"));
    assertEquals(6, collection.count());
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testWithDifferentFieldTypes(String dataStoreName) throws Exception {
    Datastore datastore = datastoreMap.get(dataStoreName);
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    // size field with integer value, isCostly boolean field
    collection.upsert(
        new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of("name", "abc1"),
            ImmutablePair.of("size", -10),
            ImmutablePair.of("isCostly", false)));

    collection.upsert(
        new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of("name", "abc2"),
            ImmutablePair.of("size", -20),
            ImmutablePair.of("isCostly", false)));

    collection.upsert(
        new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("name", "abc3"),
            ImmutablePair.of("size", 5),
            ImmutablePair.of("isCostly", true)));

    collection.upsert(
        new SingleValueKey("default", "testKey4"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey4"),
            ImmutablePair.of("name", "abc4"),
            ImmutablePair.of("size", 10),
            ImmutablePair.of("isCostly", true)));

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
    filter = new Filter(Op.GT, "name", "abc1");
    queryStringField.setFilter(filter);
    results = collection.search(queryBooleanField);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(2, documents.size());

    datastore.deleteCollection(COLLECTION_NAME);
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testNotEquals(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    collection.upsert(
        new SingleValueKey("default", "testKey1"),
        Utils.createDocument(ImmutablePair.of("key1", "abc1"), ImmutablePair.of("key2", "xyz1")));
    collection.upsert(
        new SingleValueKey("default", "testKey2"),
        Utils.createDocument(ImmutablePair.of("key1", "abc2"), ImmutablePair.of("key2", "xyz2")));
    collection.upsert(
        new SingleValueKey("default", "testKey3"),
        Utils.createDocument(ImmutablePair.of("key1", "abc3"), ImmutablePair.of("key2", "xyz3")));
    collection.upsert(
        new SingleValueKey("default", "testKey4"),
        Utils.createDocument(ImmutablePair.of("key1", "abc4")));

    collection.updateSubDoc(
        new SingleValueKey("default", "testKey1"),
        "subdoc",
        Utils.createDocument("nestedkey1", "pqr1"));
    collection.updateSubDoc(
        new SingleValueKey("default", "testKey2"),
        "subdoc",
        Utils.createDocument("nestedkey1", "pqr2"));
    collection.updateSubDoc(
        new SingleValueKey("default", "testKey3"),
        "subdoc",
        Utils.createDocument("nestedkey1", "pqr3"));

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
      documents.forEach(
          document -> {
            String jsonStr = document.toJson();
            assertTrue(
                jsonStr.contains("\"key1\":\"abc1\"")
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
      documents.forEach(
          document -> {
            String jsonStr = document.toJson();
            assertTrue(
                jsonStr.contains("\"key1\":\"abc1\"")
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
      documents.forEach(
          document -> {
            String jsonStr = document.toJson();
            assertTrue(
                jsonStr.contains("\"key1\":\"abc1\"")
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
      documents.forEach(
          document -> {
            String jsonStr = document.toJson();
            assertTrue(
                jsonStr.contains("\"key1\":\"abc1\"")
                    || document.toJson().contains("\"key1\":\"abc3\"")
                    || document.toJson().contains("\"key1\":\"abc4\""));
          });
    }
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testNotInQueryWithNumberField(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of("name", "abc1"),
            ImmutablePair.of("size", -10.2),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("tags", List.of("black", "white")),
            ImmutablePair.of("color", "red")));
    collection.upsert(
        new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of("name", "abc2"),
            ImmutablePair.of("size", 10.4),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("tags", List.of("gray")),
            ImmutablePair.of("color", "gray")));
    collection.upsert(
        new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("name", "abc3"),
            ImmutablePair.of("size", 30),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("tags", List.of("brown")),
            ImmutablePair.of("color", "blue")));
    collection.upsert(
        new SingleValueKey("default", "testKey4"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey4"),
            ImmutablePair.of("name", "abc4"),
            ImmutablePair.of("size", 10.4),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("tags", List.of("gray")),
            ImmutablePair.of("color", "pink")));

    collection.upsert(
        new SingleValueKey("default", "testKey5"),
        Utils.createDocument(ImmutablePair.of("id", "testKey5"), ImmutablePair.of("name", "abc5")));

    collection.updateSubDoc(
        new SingleValueKey("default", "testKey1"),
        "subdoc",
        Utils.createDocument("nestedkey1", "pqr1"));
    collection.updateSubDoc(
        new SingleValueKey("default", "testKey2"),
        "subdoc",
        Utils.createDocument("nestedkey1", "pqr2"));
    collection.updateSubDoc(
        new SingleValueKey("default", "testKey3"),
        "subdoc",
        Utils.createDocument("nestedkey1", "pqr3"));

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
    documents.forEach(
        document -> {
          String jsonStr = document.toJson();
          assertTrue(
              jsonStr.contains("\"name\":\"abc1\"")
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
    documents.forEach(
        document -> {
          String jsonStr = document.toJson();
          assertTrue(
              jsonStr.contains("\"name\":\"abc2\"")
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
    documents.forEach(
        document -> {
          String jsonStr = document.toJson();
          assertTrue(
              jsonStr.contains("\"name\":\"abc3\"") || jsonStr.contains("\"name\":\"abc5\""));
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
    documents.forEach(
        document -> {
          String jsonStr = document.toJson();
          assertTrue(
              jsonStr.contains("\"name\":\"abc3\"")
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
    documents.forEach(
        document -> {
          String jsonStr = document.toJson();
          assertTrue(
              jsonStr.contains("\"name\":\"abc3\"")
                  || jsonStr.contains("\"name\":\"abc4\"")
                  || jsonStr.contains("\"name\":\"abc5\""));
        });
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testSubDocumentUpdate(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));

    Document subDocument = Utils.createDocument("subfoo1", "subbar1");
    collection.updateSubDoc(docKey, "subdoc", subDocument);

    Document nestedDocument = Utils.createDocument("nestedfoo1", "nestedbar1");
    collection.updateSubDoc(docKey, "subdoc.nesteddoc", nestedDocument);

    Query query = new Query();
    query.setFilter(Filter.eq(getId(dataStoreName), "default:testKey"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertFalse(documents.isEmpty());

    // mongo
    // {"_lastUpdateTime":{"$date":"2021-03-14T18:53:14.914Z"},"createdTime":1615747994870,
    // "foo1":"bar1","lastUpdatedTime":1615747994920,"subdoc":{"subfoo1":"subbar1",
    // "nesteddoc":{"nestedfoo1":"nestedbar1"}}}

    // postgres
    // {"foo1":"bar1","subdoc":{"subfoo1":"subbar1","nesteddoc":{"nestedfoo1":"nestedbar1"}},
    // "created_at":"2021-03-15 00:24:50.981147","updated_at":"2021-03-15 00:24:50.981147"}
    System.out.println(documents.get(0).toJson());
    ObjectNode jsonNode = (ObjectNode) OBJECT_MAPPER.readTree(documents.get(0).toJson());
    String expected =
        "{\"foo1\":\"bar1\",\"subdoc\":{\"subfoo1\":\"subbar1\","
            + "\"nesteddoc\":{\"nestedfoo1\":\"nestedbar1\"}}}";
    if (isMongo(dataStoreName)) {
      jsonNode.remove(MONGO_CREATED_TIME_KEY);
      jsonNode.remove(MONGO_LAST_UPDATE_TIME_KEY);
      jsonNode.remove(MONGO_LAST_UPDATED_TIME_KEY);
    } else if (isPostgress(dataStoreName)) {
      jsonNode.remove(POSTGRES_CREATED_AT);
      jsonNode.remove(POSTGRES_UPDATED_AT);
    }
    Assertions.assertEquals(expected, OBJECT_MAPPER.writeValueAsString(jsonNode));
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void bulkUpdateSubDocForNonExistingDocuments(String dataStoreName) throws Exception {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Map<String, Document>> toUpdate = new HashMap<>();
    Key key1 = new SingleValueKey("tenant-1", "testKey1");
    Key key2 = new SingleValueKey("tenant-2", "testKey2");
    Map<String, Document> subDoc1 = new HashMap<>(), subDoc2 = new HashMap<>();
    subDoc1.put("subDocPath1", Utils.createDocument("timestamp", "100"));
    subDoc2.put("subDocPath2", Utils.createDocument("timestamp", "100"));
    toUpdate.put(key1, subDoc1);
    toUpdate.put(key2, subDoc2);
    BulkUpdateResult bulkUpdateResult = collection.bulkUpdateSubDocs(toUpdate);
    long result = bulkUpdateResult.getUpdatedCount();
    assertEquals(0, result);
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void bulkUpdateSubDocForEmptyMap(String dataStoreName) throws Exception {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Map<Key, Map<String, Document>> toUpdate = new HashMap<>();
    BulkUpdateResult bulkUpdateResult = collection.bulkUpdateSubDocs(toUpdate);
    long result = bulkUpdateResult.getUpdatedCount();
    assertEquals(0, result);
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void bulkUpdateSubDocOnlyForExistingDocuments(String dataStoreName) throws Exception {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    Key key1 = new SingleValueKey("tenant-1", "testKey1");
    Key key2 = new SingleValueKey("tenant-2", "testKey2");
    Key key3 = new SingleValueKey("tenant-3", "testKey3");
    Key key4 = new SingleValueKey("tenant-4", "testKey4");
    Key key5 = new SingleValueKey("tenant-5", "testKey5");
    collection.upsert(key1, Utils.createDocument("foo1", "bar1"));
    collection.upsert(key3, Utils.createDocument("foo3", "bar3"));
    collection.upsert(key4, Utils.createDocument("foo4", "bar4"));
    collection.upsert(key5, Utils.createDocument("different5", "bar5"));

    Map<Key, Map<String, Document>> toUpdate = new HashMap<>();
    Map<String, Document> subDoc1 = new HashMap<>(),
        subDoc2 = new HashMap<>(),
        subDoc3 = new HashMap<>(),
        subDoc4 = new HashMap<>(),
        subDoc5 = new HashMap<>();
    subDoc1.put("subDocPath1", Utils.createDocument("nested1", "100"));
    subDoc2.put("subDocPath2", Utils.createDocument("nested2", "100"));
    subDoc5.put("foo5", Utils.createDocument(ImmutablePair.of("nested5", List.of())));
    // update on already existing subDocPath
    subDoc3.put("foo3", Utils.createDocument("nested3", "100"));
    ObjectNode emptyValuedNode = OBJECT_MAPPER.createObjectNode();
    emptyValuedNode.set("someKey", OBJECT_MAPPER.createObjectNode());

    ObjectNode nested4ValueNode = OBJECT_MAPPER.createObjectNode();
    nested4ValueNode.set("nested4", emptyValuedNode);
    subDoc4.put("foo4", new JSONDocument(nested4ValueNode));

    toUpdate.put(key1, subDoc1);
    toUpdate.put(key2, subDoc2);
    toUpdate.put(key3, subDoc3);
    toUpdate.put(key4, subDoc4);
    toUpdate.put(key5, subDoc5);
    BulkUpdateResult bulkUpdateResult = collection.bulkUpdateSubDocs(toUpdate);
    long result = bulkUpdateResult.getUpdatedCount();
    assertEquals(4, result);

    Query query = new Query();
    query.setFilter(new Filter(Op.EQ, "_id", key1.toString()));
    Iterator<Document> it = collection.search(query);
    JsonNode root = OBJECT_MAPPER.readTree(it.next().toJson());
    String nestedTimestamp = root.findValue("subDocPath1").toString();
    assertEquals("{\"nested1\":\"100\"}", nestedTimestamp);

    query = new Query();
    query.setFilter(new Filter(Op.EQ, "_id", key3.toString()));
    it = collection.search(query);
    root = OBJECT_MAPPER.readTree(it.next().toJson());
    nestedTimestamp = root.findValue("foo3").toString();
    assertEquals("{\"nested3\":\"100\"}", nestedTimestamp);

    query = new Query();
    query.setFilter(new Filter(Op.EQ, "_id", key4.toString()));
    it = collection.search(query);
    root = OBJECT_MAPPER.readTree(it.next().toJson());
    String nestedValue = root.findValue("foo4").toString();
    assertEquals("{\"nested4\":{\"someKey\":{}}}", nestedValue);

    query = new Query();
    query.setFilter(new Filter(Op.EQ, "_id", key5.toString()));
    it = collection.search(query);
    root = OBJECT_MAPPER.readTree(it.next().toJson());
    nestedValue = root.findValue("foo5").toString();
    assertEquals("{\"nested5\":[]}", nestedValue);

    query = new Query();
    query.setFilter(new Filter(Op.EQ, "_id", key2.toString()));
    it = collection.search(query);
    assertFalse(it.hasNext());
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testSubDocumentDelete(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
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

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testCount(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));
    Assertions.assertEquals(collection.count(), 1);
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testIgnoreCaseLikeQuery(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    long now = Instant.now().toEpochMilli();
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey"), Utils.createDocument("name", "Bob"));

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
      JsonNode jsonNode = OBJECT_MAPPER.reader().readTree(persistedDocument);
      Assertions.assertTrue(persistedDocument.contains("Bob"));
      if (isMongo(dataStoreName)) {
        Assertions.assertTrue(jsonNode.findValue("createdTime").asLong(0) >= now);
        Assertions.assertTrue(jsonNode.findValue("lastUpdatedTime").asLong(0) >= now);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testExistsFilter(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of("name", "abc1"),
            ImmutablePair.of("size", -10.2),
            ImmutablePair.of("isCostly", false)));
    collection.upsert(
        new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of("name", "abc2"),
            ImmutablePair.of("size", 10.4),
            ImmutablePair.of("isCostly", false)));
    collection.upsert(
        new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("name", "abc3"),
            ImmutablePair.of("size", 30),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("city", "bangalore")));
    collection.upsert(
        new SingleValueKey("default", "testKey4"),
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

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testNotExistsFilter(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey1"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey1"),
            ImmutablePair.of("name", "abc1"),
            ImmutablePair.of("size", -10.2),
            ImmutablePair.of("isCostly", false)));
    collection.upsert(
        new SingleValueKey("default", "testKey2"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey2"),
            ImmutablePair.of("name", "abc2"),
            ImmutablePair.of("size", 10.4),
            ImmutablePair.of("isCostly", false)));
    collection.upsert(
        new SingleValueKey("default", "testKey3"),
        Utils.createDocument(
            ImmutablePair.of("id", "testKey3"),
            ImmutablePair.of("name", "abc3"),
            ImmutablePair.of("size", 30),
            ImmutablePair.of("isCostly", false),
            ImmutablePair.of("city", "bangalore")));
    collection.upsert(
        new SingleValueKey("default", "testKey4"),
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

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testTotalWithQuery(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey1"), Utils.createDocument("name", "Bob"));
    collection.upsert(
        new SingleValueKey("default", "testKey2"), Utils.createDocument("name", "Alice"));
    collection.upsert(
        new SingleValueKey("default", "testKey3"), Utils.createDocument("name", "Alice"));
    collection.upsert(
        new SingleValueKey("default", "testKey4"), Utils.createDocument("name", "Bob"));
    collection.upsert(
        new SingleValueKey("default", "testKey5"), Utils.createDocument("name", "Alice"));
    collection.upsert(
        new SingleValueKey("default", "testKey6"),
        Utils.createDocument("email", "bob@example.com"));

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

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testOffsetAndLimitOrderBy(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey1"), Utils.createDocument("foo1", "bar1"));
    collection.upsert(
        new SingleValueKey("default", "testKey2"), Utils.createDocument("foo2", "bar2"));
    collection.upsert(
        new SingleValueKey("default", "testKey3"), Utils.createDocument("foo3", "bar3"));
    collection.upsert(
        new SingleValueKey("default", "testKey4"), Utils.createDocument("foo4", "bar4"));
    collection.upsert(
        new SingleValueKey("default", "testKey5"), Utils.createDocument("foo5", "bar5"));

    // Querying 5 times, to make sure the order of results is maintained with offset + limit
    for (int i = 0; i < 5; i++) {
      Query query = new Query();
      query.setLimit(2);
      query.setOffset(1);
      query.addOrderBy(new OrderBy("_id", true));
      query.addOrderBy(new OrderBy("foo1", true));
      query.addOrderBy(new OrderBy("foo2", true));
      query.addOrderBy(new OrderBy("foo3", true));

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

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testDelete(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));

    Assertions.assertEquals(collection.count(), 1);
    collection.delete(docKey);
    Assertions.assertEquals(collection.count(), 0);
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testBulkDelete(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey1 = new SingleValueKey("default", "testKey1");
    collection.upsert(docKey1, Utils.createDocument("foo1", "bar1"));
    SingleValueKey docKey2 = new SingleValueKey("default", "testKey2");
    collection.upsert(docKey2, Utils.createDocument("foo2", "bar2"));
    SingleValueKey docKey3 = new SingleValueKey("default", "testKey3");
    collection.upsert(docKey3, Utils.createDocument("foo3", "bar3"));

    Assertions.assertEquals(collection.count(), 3);
    collection.delete(Set.of(docKey1, docKey2));
    Assertions.assertEquals(collection.count(), 1);
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testDeleteAll(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey docKey = new SingleValueKey("default", "testKey");
    collection.upsert(docKey, Utils.createDocument("foo1", "bar1"));

    Assertions.assertEquals(collection.count(), 1);
    collection.deleteAll();
    Assertions.assertEquals(collection.count(), 0);
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testInQuery(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    collection.upsert(
        new SingleValueKey("default", "testKey1"), Utils.createDocument("name", "Bob"));
    collection.upsert(
        new SingleValueKey("default", "testKey2"), Utils.createDocument("name", "Alice"));
    collection.upsert(
        new SingleValueKey("default", "testKey3"), Utils.createDocument("name", "Halo"));

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

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testSearchForNestedKey(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    String documentString =
        "{\"attributes\":{\"trace_id\":{\"value\":{\"string\":\"00000000000000005e194fdf9fbf5101"
            + "\"}},\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},"
            + "\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},"
            + "\"FQN\":{\"value\":{\"string\":\"driver\"}}},"
            + "\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\","
            + "\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string"
            + "\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document = new JSONDocument(documentString);
    SingleValueKey key = new SingleValueKey("default", "testKey1");
    collection.upsert(key, document);

    // Search nested field in the document
    Query query = new Query();
    query.setFilter(
        new Filter(Filter.Op.EQ, "attributes.span_id.value.string", "6449f1f720c93a67"));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertEquals(documents.size(), 1);
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testSearch(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    String docStr1 =
        "{\"amount\":1234.5,\"testKeyExist\":null,"
            + "\"attributes\":{\"trace_id\":{\"value\":{\"string"
            + "\":\"00000000000000005e194fdf9fbf5101\"}},"
            + "\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},"
            + "\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},"
            + "\"FQN\":{\"value\":{\"string\":\"driver\"}}},"
            + "\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\","
            + "\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string"
            + "\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document1 = new JSONDocument(docStr1);
    SingleValueKey key1 = new SingleValueKey("default", "testKey1");
    collection.upsert(key1, document1);

    String docStr2 =
        "{\"amount\":1234,\"testKeyExist\":123,"
            + "\"attributes\":{\"trace_id\":{\"value\":{\"testKeyExistNested\":123,"
            + "\"string\":\"00000000000000005e194fdf9fbf5101\"}},"
            + "\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},"
            + "\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},"
            + "\"FQN\":{\"value\":{\"string\":\"driver\"}}},"
            + "\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\","
            + "\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string"
            + "\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document2 = new JSONDocument(docStr2);
    SingleValueKey key2 = new SingleValueKey("default", "testKey2");
    collection.upsert(key2, document2);

    String docStr3 =
        "{\"attributes\":{\"trace_id\":{\"value\":{\"testKeyExistNested\":null,"
            + "\"string\":\"00000000000000005e194fdf9fbf5101\"}},"
            + "\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},"
            + "\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},"
            + "\"FQN\":{\"value\":{\"string\":\"driver\"}}},"
            + "\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\","
            + "\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string"
            + "\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document3 = new JSONDocument(docStr3);
    SingleValueKey key3 = new SingleValueKey("default", "testKey3");
    collection.upsert(key3, document3);

    // Search integer field
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, "amount", 1234));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
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
      while (results.hasNext()) {
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
      while (results.hasNext()) {
        documents.add(results.next());
      }
      Assertions.assertEquals(2, documents.size());
    }

    // Search _id field in the document
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, "_id", key1.toString()));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      Assertions.assertEquals(1, documents.size());
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
      query.setFilter(
          new Filter(Op.NOT_EXISTS, "attributes.trace_id.value.testKeyExistNested", null));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      Assertions.assertEquals(1, documents.size());
    }

    // Unsupported Object Type in Filter
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, "amount", new Filter()));

      Exception exception;
      String expected;
      if (isMongo(dataStoreName)) {
        assertThrows(CodecConfigurationException.class, () -> collection.search(query));
      } else if (isPostgress(dataStoreName)) {
        // should throw an UnsupportedOperationException
        exception =
            assertThrows(UnsupportedOperationException.class, () -> collection.search(query));
        expected = "Un-supported object types in filter";
        assertTrue(exception.getMessage().contains(expected));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testCreateWithMultipleThreads(String dataStoreName) throws Exception {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    SingleValueKey documentKey = new SingleValueKey("default", "testKey1");

    Map<String, List<CreateUpdateTestThread>> resultMap =
        executeCreateUpdateThreads(collection, Operation.CREATE, 5, documentKey);

    Assertions.assertEquals(1, resultMap.get(SUCCESS).size());
    Assertions.assertEquals(4, resultMap.get(FAILURE).size());

    // check the inserted document and thread result matches
    Query query = new Query();
    query.setFilter(Filter.eq("_id", documentKey.toString()));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 1);
    Map<String, Object> doc = OBJECT_MAPPER.readValue(documents.get(0).toJson(), Map.class);
    Assertions.assertEquals(resultMap.get(SUCCESS).get(0).getTestValue(), (int) doc.get("size"));
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testUpdateWithMultipleThreads(String dataStoreName) throws Exception {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    SingleValueKey documentKey = new SingleValueKey("default", "testKey1");

    // create document
    Document document =
        Utils.createDocument(
            ImmutablePair.of("id", documentKey.getValue()),
            ImmutablePair.of("name", "inserted"),
            ImmutablePair.of("size", RandomUtils.nextInt(1, 6)));
    CreateResult createResult = collection.create(documentKey, document);
    Assertions.assertTrue(createResult.isSucceed());

    Map<String, List<CreateUpdateTestThread>> resultMap =
        executeCreateUpdateThreads(collection, Operation.UPDATE, 5, documentKey);

    Assertions.assertEquals(1, resultMap.get(SUCCESS).size());
    Assertions.assertEquals(4, resultMap.get(FAILURE).size());

    // check the inserted document and thread result matches
    Query query = new Query();
    query.setFilter(Filter.eq("_id", documentKey.toString()));
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 1);
    Map<String, Object> doc = OBJECT_MAPPER.readValue(documents.get(0).toJson(), Map.class);
    Assertions.assertEquals(
        "thread-" + resultMap.get(SUCCESS).get(0).getTestValue(), doc.get("name"));
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testUpdateWithCondition(String dataStoreName) throws Exception {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    Query query = new Query();
    query.setFilter(Filter.eq("_id", "default:testKey1"));
    Filter condition = new Filter(Op.EQ, "isCostly", false);

    // test that document is inserted if its not exists
    Iterator<Document> results = collection.search(query);
    List<Document> documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 0);

    CreateResult createResult =
        collection.create(
            new SingleValueKey("default", "testKey1"),
            Utils.createDocument(
                ImmutablePair.of("id", "testKey1"),
                ImmutablePair.of("name", "abc1"),
                ImmutablePair.of("size", -10),
                ImmutablePair.of("isCostly", false)));

    Assertions.assertTrue(createResult.isSucceed());

    // test that document is updated if condition met
    UpdateResult updateResult =
        collection.update(
            new SingleValueKey("default", "testKey1"),
            Utils.createDocument(
                ImmutablePair.of("id", "testKey1"),
                ImmutablePair.of("name", "abc1"),
                ImmutablePair.of("size", 10),
                ImmutablePair.of("isCostly", false)),
            condition);

    Assertions.assertTrue(updateResult.getUpdatedCount() == 1);

    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 1);
    Map<String, Object> doc = OBJECT_MAPPER.readValue(documents.get(0).toJson(), Map.class);
    Assertions.assertEquals(10, (int) doc.get("size"));

    // test that document is not updated if condition not met
    condition = new Filter(Op.EQ, "isCostly", true);
    try {
      updateResult =
          collection.update(
              new SingleValueKey("default", "testKey1"),
              Utils.createDocument(
                  ImmutablePair.of("id", "testKey1"),
                  ImmutablePair.of("name", "abc1"),
                  ImmutablePair.of("size", 20),
                  ImmutablePair.of("isCostly", true)),
              condition);
    } catch (IOException e) {
      updateResult = new UpdateResult(0);
    }

    Assertions.assertTrue(updateResult.getUpdatedCount() == 0);

    results = collection.search(query);
    documents = new ArrayList<>();
    while (results.hasNext()) {
      documents.add(results.next());
    }
    Assertions.assertTrue(documents.size() == 1);
    doc = OBJECT_MAPPER.readValue(documents.get(0).toJson(), Map.class);
    Assertions.assertEquals(10, (int) doc.get("size"));
    Assertions.assertFalse((boolean) doc.get("isCostly"));
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testSearchIteratorInterface(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    String docStr1 =
        "{\"amount\":1234.5,\"testKeyExist\":null,"
            + "\"attributes\":{\"trace_id\":{\"value\":{\"string"
            + "\":\"00000000000000005e194fdf9fbf5101\"}},"
            + "\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},"
            + "\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},"
            + "\"FQN\":{\"value\":{\"string\":\"driver\"}}},"
            + "\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\","
            + "\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string"
            + "\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document1 = new JSONDocument(docStr1);
    SingleValueKey key1 = new SingleValueKey("default", "testKey1");
    collection.upsert(key1, document1);

    String docStr2 =
        "{\"amount\":1234,\"testKeyExist\":123,"
            + "\"attributes\":{\"trace_id\":{\"value\":{\"testKeyExistNested\":123,"
            + "\"string\":\"00000000000000005e194fdf9fbf5101\"}},"
            + "\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},"
            + "\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},"
            + "\"FQN\":{\"value\":{\"string\":\"driver\"}}},"
            + "\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\","
            + "\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string"
            + "\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document2 = new JSONDocument(docStr2);
    SingleValueKey key2 = new SingleValueKey("default", "testKey2");
    collection.upsert(key2, document2);

    String docStr3 =
        "{\"attributes\":{\"trace_id\":{\"value\":{\"testKeyExistNested\":null,"
            + "\"string\":\"00000000000000005e194fdf9fbf5101\"}},"
            + "\"span_id\":{\"value\":{\"string\":\"6449f1f720c93a67\"}},"
            + "\"service_type\":{\"value\":{\"string\":\"JAEGER_SERVICE\"}},"
            + "\"FQN\":{\"value\":{\"string\":\"driver\"}}},"
            + "\"entityId\":\"e3ffc6f0-fc92-3a9c-9fa0-26269184d1aa\",\"entityName\":\"driver\","
            + "\"entityType\":\"SERVICE\",\"identifyingAttributes\":{\"FQN\":{\"value\":{\"string"
            + "\":\"driver\"}}},\"tenantId\":\"__default\"}";
    Document document3 = new JSONDocument(docStr3);
    SingleValueKey key3 = new SingleValueKey("default", "testKey3");
    collection.upsert(key3, document3);

    // Search _id field in the document
    // Creates the pattern where [hashNext, hashNext, next, hasNext, next] is called.
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, "_id", key1.toString()));
      Iterator<Document> results = collection.search(query);
      if (!results.hasNext()) {
        Assertions.fail();
      }
      List<Document> documents = new ArrayList<>();
      while (results.hasNext()) {
        documents.add(results.next());
      }
      Assertions.assertEquals(1, documents.size());
    }

    // Search _id field in the document
    // Creates the pattern where [next, hashNext] is called.
    {
      Query query = new Query();
      query.setFilter(new Filter(Filter.Op.EQ, "_id", key1.toString()));
      Iterator<Document> results = collection.search(query);
      List<Document> documents = new ArrayList<>();
      while (true) {
        documents.add(results.next());
        if (!results.hasNext()) {
          break;
        }
      }
      Assertions.assertEquals(1, documents.size());
    }
  }

  @ParameterizedTest
  @MethodSource("databaseContextProvider")
  public void testNewAggregateApiWhereClause(String dataStoreName) throws IOException {
    Map<Key, Document> documents = createDocumentsFromResource("mongo/collection_data.json");
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    // add docs
    boolean result = collection.bulkUpsert(documents);
    Assertions.assertTrue(result);

    // query docs
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertSizeAndDocsEqual(dataStoreName, iterator, 6, "mongo/simple_filter_quantity_neq_10.json");
  }

  private Map<String, List<CreateUpdateTestThread>> executeCreateUpdateThreads(
      Collection collection, Operation operation, int numThreads, SingleValueKey documentKey) {
    List<CreateUpdateTestThread> threads = new ArrayList<CreateUpdateTestThread>();
    IntStream.range(1, numThreads + 1)
        .forEach(
            number ->
                threads.add(
                    new CreateUpdateTestThread(collection, documentKey, number, operation)));
    threads.stream().forEach(t -> t.start());
    threads.stream()
        .forEach(
            t -> {
              try {
                t.join();
              } catch (InterruptedException ex) {
                // result of such threads are consider as failure
              }
            });

    Map<String, List<CreateUpdateTestThread>> resultMap =
        new HashMap<>() {
          {
            put(SUCCESS, new ArrayList());
            put(FAILURE, new ArrayList());
          }
        };

    threads.stream()
        .forEach(
            t -> {
              if (t.getTestResult()) {
                resultMap.get(SUCCESS).add(t);
              } else {
                resultMap.get(FAILURE).add(t);
              }
            });
    return resultMap;
  }

  /**
   * mongo {"_lastUpdateTime":{"$date":"2021-03-14T15:43:04.842Z"},"createdTime":1615736584763,
   * "foo1":"bar1","lastUpdatedTime":1615736584763} postgres {"foo1":"bar1","created_at":"2021-03-14
   * 21:20:00.178909","updated_at":"2021-03-14 21:20:00.178909"}
   */
  private static void verifyTimeRelatedFieldsPresent(String doc, String dataStoreName) {
    if (isMongo(dataStoreName)) {
      assertTrue(doc.contains(MONGO_LAST_UPDATE_TIME_KEY));
      assertTrue(doc.contains(MONGO_CREATED_TIME_KEY));
      assertTrue(doc.contains(MONGO_LAST_UPDATED_TIME_KEY));
    } else if (isPostgress(dataStoreName)) {
      assertTrue(doc.contains(POSTGRES_CREATED_AT));
      assertTrue(doc.contains(POSTGRES_UPDATED_AT));
    }
  }

  private static boolean isMongo(String dataStoreName) {
    return MONGO_STORE.equals(dataStoreName);
  }

  private static boolean isPostgress(String dataStoreName) {
    return POSTGRES_STORE.equals(dataStoreName);
  }

  static Object getCreatedTime(String doc, String dataStoreName) throws Exception {
    JsonNode node = OBJECT_MAPPER.readTree(doc);
    if (isMongo(dataStoreName)) {
      return node.findValue(MONGO_CREATED_TIME_KEY).asLong();
    } else if (isPostgress(dataStoreName)) {
      return node.findValue(POSTGRES_CREATED_AT).asText();
    }
    return "";
  }

  static Object getLastUpdatedTime(String doc, String dataStoreName) throws Exception {
    JsonNode node = OBJECT_MAPPER.readTree(doc);
    if (isMongo(dataStoreName)) {
      return node.findValue(MONGO_LAST_UPDATE_TIME_KEY).findValue("$date").asText();
    } else if (isPostgress(dataStoreName)) {
      return node.findValue(POSTGRES_UPDATED_AT).asText();
    }
    return "";
  }

  static String getId(String dataStoreName) {
    if (isMongo(dataStoreName)) {
      return "_id";
    } else {
      return "id";
    }
  }

  private static void assertSizeAndDocsEqual(
      String dataStoreName, Iterator<Document> documents, int expectedSize, String filePath)
      throws IOException {
    String fileContent = readFileFromResource(filePath).orElseThrow();
    List<Map<String, Object>> expectedDocs = convertJsonToMap(fileContent);

    List<Map<String, Object>> actualDocs = new ArrayList<>();
    int actualSize = 0;
    while (documents.hasNext()) {
      Map<String, Object> doc = convertDocumentToMap(documents.next());
      removesDateRelatedFields(dataStoreName, doc);
      actualDocs.add(doc);
      actualSize++;
    }

    long count =
        expectedDocs.stream().filter(expectedDoc -> actualDocs.contains(expectedDoc)).count();
    assertEquals(expectedSize, actualSize);
    assertEquals(expectedSize, count);
  }

  private static void removesDateRelatedFields(String dataStoreName, Map<String, Object> document) {
    if (isMongo(dataStoreName)) {
      document.remove(MONGO_CREATED_TIME_KEY);
      document.remove(MONGO_LAST_UPDATED_TIME_KEY);
      document.remove(MONGO_LAST_UPDATE_TIME_KEY);
    } else if (isPostgress(dataStoreName)) {
      document.remove(POSTGRES_CREATED_AT);
      document.remove(POSTGRES_UPDATED_AT);
    }
  }
}
